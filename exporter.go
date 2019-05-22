package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jeffail/gabs"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const defaultNamespace = "marathon"

type LabelConverter struct {
	*regexp.Regexp
	MetricLabel string
}

func NewLabelConverter(label string, regex string) (*LabelConverter, error) {
	r, err := regexp.Compile(regex)
	if err != nil {
		return nil, err
	}
	return &LabelConverter{
		r,
		label,
	}, nil
}

func (lc *LabelConverter) Labels() []string {
	labels := []string{}
	for _, val := range lc.SubexpNames() {
		if val != "" {
			labels = append(labels, val)
		}
	}
	return labels
}

func (lc *LabelConverter) Convert(label string) []string {
	matches := lc.FindStringSubmatch(label)
	if matches == nil {
		return nil
	}
	values := []string{}
	for idx, name := range lc.SubexpNames() {
		if name != "" {
			values = append(values, matches[idx])
		}
	}
	return values
}

type LabelsExporter struct {
	boolConversions   map[*LabelConverter]*prometheus.Desc
	stringConversions map[*LabelConverter]*prometheus.Desc
	floatConversions  map[*LabelConverter]*prometheus.Desc
}

func NewLabelsExporter(namespace string, bool_conversions []*LabelConverter, string_conversions []*LabelConverter, float_conversions []*LabelConverter) (*LabelsExporter, error) {
	bool_map := make(map[*LabelConverter]*prometheus.Desc)
	for _, val := range bool_conversions {
		labels := []string{"app", "app_version"}
		labels = append(labels, val.Labels()...)
		bool_map[val] = prometheus.NewDesc(
			fmt.Sprintf("%s_app_label_%s", namespace, val.MetricLabel),
			fmt.Sprintf("Marathon apps with the label %s and constant gauge of 1", val),
			labels,
			prometheus.Labels{},
		)
	}

	string_map := make(map[*LabelConverter]*prometheus.Desc)
	for _, val := range string_conversions {
		labels := []string{"app", "app_version", "value"}
		labels = append(labels, val.Labels()...)
		string_map[val] = prometheus.NewDesc(
			fmt.Sprintf("%s_app_label_%s", namespace, val.MetricLabel),
			fmt.Sprintf("Marathon apps with the label %s, label value exported as 'value', and constant gauge of 1", val),
			labels,
			prometheus.Labels{},
		)
	}

	float_map := make(map[*LabelConverter]*prometheus.Desc)
	for _, val := range float_conversions {
		labels := []string{"app", "app_version"}
		labels = append(labels, val.Labels()...)
		float_map[val] = prometheus.NewDesc(
			fmt.Sprintf("%s_app_label_%s", namespace, val.MetricLabel),
			fmt.Sprintf("Marathon apps with the label %s, and label value as the gauge", val),
			labels,
			prometheus.Labels{},
		)
	}

	return &LabelsExporter{
		boolConversions:   bool_map,
		stringConversions: string_map,
		floatConversions:  float_map,
	}, nil
}

func (le *LabelsExporter) scrapeLabels(app string, app_version string, json *gabs.Container, ch chan<- prometheus.Metric) {
	app_labels := json.Path("labels").Data().(map[string]interface{})
	for label, value_i := range app_labels {
		value, ok := value_i.(string)
		if !ok {
			continue
		}

		for converter, desc := range le.boolConversions {
			metric_labels := converter.Convert(label)
			if metric_labels != nil {
				metric_labels = append([]string{app, app_version}, metric_labels...)
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, metric_labels...)
			}
		}

		for converter, desc := range le.stringConversions {
			metric_labels := converter.Convert(label)
			if metric_labels != nil {
				metric_labels = append([]string{app, app_version, value}, metric_labels...)
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, metric_labels...)
			}
		}
		// lazy convert the value; this is also to only warn if a match exists and we couldn't convert the value.
		var value_f *float64 = nil
		for converter, desc := range le.floatConversions {
			metric_labels := converter.Convert(label)
			if metric_labels != nil {
				if value_f == nil {
					if f, err := strconv.ParseFloat(value, 64); err != nil {
						log.Warnf("Failed to convert app %s label %s, value %s to float: %s", app, label, value, err)
						// if we can't convert it to a float, skip any futher attempts for this label.
						break
					} else {
						value_f = &f
					}
				}
				metric_labels = append([]string{app, app_version}, metric_labels...)
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, *value_f, metric_labels...)
			}
		}
	}

}

type Exporter struct {
	scraper        Scraper
	duration       prometheus.Gauge
	scrapeError    prometheus.Gauge
	up             prometheus.Gauge
	totalErrors    prometheus.Counter
	totalScrapes   prometheus.Counter
	Counters       *CounterContainer
	Gauges         *GaugeContainer
	labelsExporter *LabelsExporter
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	log.Debugln("Describing metrics")
	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting metrics")
	e.scrape(ch)

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.totalErrors
	ch <- e.scrapeError
	ch <- e.up
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	e.totalScrapes.Inc()

	var err error
	defer func(begin time.Time) {
		e.duration.Set(time.Since(begin).Seconds())
		if err == nil {
			e.scrapeError.Set(0)
			e.up.Set(1)
		} else {
			e.totalErrors.Inc()
			e.scrapeError.Set(1)
			e.up.Set(0)
		}
	}(time.Now())

	// Rebuild gauges & coutners to avoid stale values
	e.Gauges = NewGaugeContainer(e.Gauges.namespace)
	e.Counters = NewCounterContainer(e.Counters.namespace)
	if err = e.exportApps(ch); err != nil {
		return
	}
	if err = e.exportMetrics(ch); err != nil {
		return
	}
	if err = e.exportTasks(ch); err != nil {
		return
	}

	e.Counters.mutex.Lock()
	defer e.Counters.mutex.Unlock()
	for _, counter := range e.Counters.counters {
		counter.Collect(ch)
	}

	e.Gauges.mutex.Lock()
	defer e.Gauges.mutex.Unlock()
	for _, gauge := range e.Gauges.gauges {
		gauge.Collect(ch)
	}
}

func (e *Exporter) exportApps(ch chan<- prometheus.Metric) (err error) {
	content, err := e.scraper.Scrape("v2/apps?embed=apps.taskStats")
	if err != nil {
		log.Debugf("Problem scraping v2/apps endpoint: %v\n", err)
		return
	}

	json, err := gabs.ParseJSON(content)
	if err != nil {
		log.Debugf("Problem parsing v2/apps response: %v\n", err)
		return
	}

	e.scrapeApps(json, ch)
	return
}

func (e *Exporter) exportMetrics(ch chan<- prometheus.Metric) (err error) {
	content, err := e.scraper.Scrape("metrics")
	if err != nil {
		log.Debugf("Problem scraping metrics endpoint: %v\n", err)
		return
	}

	json, err := gabs.ParseJSON(content)
	if err != nil {
		log.Debugf("Problem parsing metrics response: %v\n", err)
		return
	}

	e.scrapeMetrics(json, ch)
	return
}

func (e *Exporter) exportTasks(ch chan<- prometheus.Metric) (err error) {
	content, err := e.scraper.Scrape("v2/tasks")
	if err != nil {
		log.Debugf("Problem scraping tasks endpoint: %v\n", err)
		return
	}

	json, err := gabs.ParseJSON(content)
	if err != nil {
		log.Debugf("Problem parsing metrics response: %v\n", err)
		return
	}

	e.scrapeTasks(json, ch)
	return
}

func (e *Exporter) scrapeTasks(json *gabs.Container, ch chan<- prometheus.Metric) {
	elements, _ := json.S("tasks").Children()

	// Create a metric on the fly to avoid staleness issues.
	desc := prometheus.NewDesc("marathon_task_state", "Task state on a per instance basis", []string{"app", "id", "slave_id", "state", "version"}, prometheus.Labels{})
	for _, task := range elements {
		labels := []string{
			task.Path("appId").Data().(string),
			task.Path("id").Data().(string),
			task.Path("slaveId").Data().(string),
			task.Path("state").Data().(string),
			task.Path("version").Data().(string),
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, labels...)
	}
}

func (e *Exporter) scrapeApps(json *gabs.Container, ch chan<- prometheus.Metric) {
	elements, _ := json.S("apps").Children()
	states := map[string]string{
		"running":    "tasksRunning",
		"staged":     "tasksStaged",
		"healthy":    "tasksHealthy",
		"unhealthy":  "tasksUnhealthy",
		"cpus":       "cpus",
		"mem_in_mb":  "mem",
		"disk_in_mb": "disk",
		"gpus":       "gpus",
		"avg_uptime": "taskStats.startedAfterLastScaling.stats.lifeTime.averageSeconds",
	}

	name := "app_instances"
	gauge, new := e.Gauges.Fetch(name, "Marathon app instance count", "app", "app_version")
	if new {
		log.Infof("Added gauge %q\n", name)
	}
	gauge.Reset()

	for _, app := range elements {
		id := app.Path("id").Data().(string)
		version := app.Path("version").Data().(string)
		data := app.Path("instances").Data()
		count, ok := data.(float64)
		if !ok {
			log.Debugf(fmt.Sprintf("Bad conversion! Unexpected value \"%v\" for number of app instances\n", data))
			continue
		}

		gauge.WithLabelValues(id, version).Set(count)

		for key, value := range states {
			name := fmt.Sprintf("app_task_%s", key)
			gauge, new := e.Gauges.Fetch(name, fmt.Sprintf("Marathon app task %s count", key), "app", "app_version")
			if new {
				log.Infof("Added gauge %q\n", name)
			}

			data := app.Path(value).Data()
			count, ok := data.(float64)
			if !ok {
				log.Debugf(fmt.Sprintf("Bad conversion! Unexpected value \"%v\" for number of \"%s\" tasks\n", data, key))
				continue
			}

			gauge.WithLabelValues(id, version).Set(count)
		}
		e.labelsExporter.scrapeLabels(id, version, app, ch)
	}
}

func (e *Exporter) scrapeMetrics(json *gabs.Container, ch chan<- prometheus.Metric) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		switch key {
		case "message":
			log.Errorf("Problem collecting metrics: %s\n", element.Data().(string))
			return
		case "version":
			data := element.Data()
			version, ok := data.(string)
			if !ok {
				log.Errorf(fmt.Sprintf("Bad conversion! Unexpected value \"%v\" for version\n", data))
			} else {
				gauge, _ := e.Gauges.Fetch("metrics_version", "Marathon metrics version", "version")
				gauge.WithLabelValues(version).Set(1)
				gauge.Collect(ch)
			}

		case "counters":
			e.scrapeCounters(element)
		case "gauges":
			e.scrapeGauges(element)
		case "histograms":
			e.scrapeHistograms(element)
		case "meters":
			e.scrapeMeters(element)
		case "timers":
			e.scrapeTimers(element)
		}
	}
}

func (e *Exporter) scrapeCounters(json *gabs.Container) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		new, err := e.scrapeCounter(key, element)
		if err != nil {
			log.Debug(err)
		} else if new {
			log.Infof("Added counter %q\n", key)
		}
	}
}

func (e *Exporter) scrapeCounter(key string, json *gabs.Container) (bool, error) {
	data := json.Path("count").Data()
	count, ok := data.(float64)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad conversion! Unexpected value \"%v\" for counter %s\n", data, key))
	}

	name := renameMetric(key)
	help := fmt.Sprintf(counterHelp, key)
	counter, new := e.Counters.Fetch(name, help)
	counter.WithLabelValues().Set(count)
	return new, nil
}

func (e *Exporter) scrapeGauges(json *gabs.Container) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		new, err := e.scrapeGauge(key, element)
		if err != nil {
			log.Debug(err)
		} else if new {
			log.Infof("Added gauge %q\n", key)
		}
	}
}

func (e *Exporter) scrapeGauge(key string, json *gabs.Container) (bool, error) {
	data := json.Path("max").Data()
	value, ok := data.(float64)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad conversion! Unexpected value \"%v\" for gauge %s\n", data, key))
	}

	name := renameMetric(key)
	help := fmt.Sprintf(gaugeHelp, key)
	gauge, new := e.Gauges.Fetch(name, help)
	gauge.WithLabelValues().Set(value)
	return new, nil
}

func (e *Exporter) scrapeMeters(json *gabs.Container) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		new, err := e.scrapeMeter(key, element)
		if err != nil {
			log.Debug(err)
		} else if new {
			log.Infof("Added meter %q\n", key)
		}
	}
}

func (e *Exporter) scrapeMeter(key string, json *gabs.Container) (bool, error) {
	count, ok := json.Path("count").Data().(float64)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad meter! %s has no count\n", key))
	}
	units, ok := json.Path("units").Data().(string)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad meter! %s has no units\n", key))
	}

	name := renameMetric(key)
	help := fmt.Sprintf(meterHelp, key, units)
	counter, new := e.Counters.Fetch(name+"_count", help)
	counter.WithLabelValues().Set(count)

	gauge, _ := e.Gauges.Fetch(name, help, "rate")
	properties, _ := json.ChildrenMap()
	for key, property := range properties {
		if strings.Contains(key, "rate") {
			if value, ok := property.Data().(float64); ok {
				gauge.WithLabelValues(renameRate(key)).Set(value)
			}
		}
	}

	return new, nil
}

func (e *Exporter) scrapeHistograms(json *gabs.Container) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		new, err := e.scrapeHistogram(key, element)
		if err != nil {
			log.Debug(err)
		} else if new {
			log.Infof("Added histogram %q\n", key)
		}
	}
}

func (e *Exporter) scrapeHistogram(key string, json *gabs.Container) (bool, error) {
	count, ok := json.Path("count").Data().(float64)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad historgram! %s has no count\n", key))
	}

	name := renameMetric(key)
	help := fmt.Sprintf(histogramHelp, key)
	counter, new := e.Counters.Fetch(name+"_count", help)
	counter.WithLabelValues().Set(count)

	percentiles, _ := e.Gauges.Fetch(name, help, "percentile")
	max, _ := e.Gauges.Fetch(name+"_max", help)
	mean, _ := e.Gauges.Fetch(name+"_mean", help)
	min, _ := e.Gauges.Fetch(name+"_min", help)
	stddev, _ := e.Gauges.Fetch(name+"_stddev", help)

	properties, _ := json.ChildrenMap()
	for key, property := range properties {
		switch key {
		case "p50", "p75", "p95", "p98", "p99", "p999":
			if value, ok := property.Data().(float64); ok {
				percentiles.WithLabelValues("0." + key[1:]).Set(value)
			}
		case "min":
			if value, ok := property.Data().(float64); ok {
				min.WithLabelValues().Set(value)
			}
		case "max":
			if value, ok := property.Data().(float64); ok {
				max.WithLabelValues().Set(value)
			}
		case "mean":
			if value, ok := property.Data().(float64); ok {
				mean.WithLabelValues().Set(value)
			}
		case "stddev":
			if value, ok := property.Data().(float64); ok {
				stddev.WithLabelValues().Set(value)
			}
		}
	}

	return new, nil
}

func (e *Exporter) scrapeTimers(json *gabs.Container) {
	elements, _ := json.ChildrenMap()
	for key, element := range elements {
		new, err := e.scrapeTimer(key, element)
		if err != nil {
			log.Debug(err)
		} else if new {
			log.Infof("Added timer %q\n", key)
		}
	}
}

func (e *Exporter) scrapeTimer(key string, json *gabs.Container) (bool, error) {
	count, ok := json.Path("count").Data().(float64)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad timer! %s has no count\n", key))
	}
	units, ok := json.Path("rate_units").Data().(string)
	if !ok {
		return false, errors.New(fmt.Sprintf("Bad timer! %s has no units\n", key))
	}

	name := renameMetric(key)
	help := fmt.Sprintf(timerHelp, key, units)
	counter, new := e.Counters.Fetch(name+"_count", help)
	counter.WithLabelValues().Set(count)

	rates, _ := e.Gauges.Fetch(name+"_rate", help, "rate")
	percentiles, _ := e.Gauges.Fetch(name, help, "percentile")
	min, _ := e.Gauges.Fetch(name+"_min", help)
	max, _ := e.Gauges.Fetch(name+"_max", help)
	mean, _ := e.Gauges.Fetch(name+"_mean", help)
	stddev, _ := e.Gauges.Fetch(name+"_stddev", help)

	properties, _ := json.ChildrenMap()
	for key, property := range properties {
		switch key {
		case "mean_rate", "m1_rate", "m5_rate", "m15_rate":
			if value, ok := property.Data().(float64); ok {
				rates.WithLabelValues(renameRate(key)).Set(value)
			}

		case "p50", "p75", "p95", "p98", "p99", "p999":
			if value, ok := property.Data().(float64); ok {
				percentiles.WithLabelValues("0." + key[1:]).Set(value)
			}
		case "min":
			if value, ok := property.Data().(float64); ok {
				min.WithLabelValues().Set(value)
			}
		case "max":
			if value, ok := property.Data().(float64); ok {
				max.WithLabelValues().Set(value)
			}
		case "mean":
			if value, ok := property.Data().(float64); ok {
				mean.WithLabelValues().Set(value)
			}
		case "stddev":
			if value, ok := property.Data().(float64); ok {
				stddev.WithLabelValues().Set(value)
			}
		}
	}

	return new, nil
}

func NewExporter(s Scraper, namespace string, labelBools []*LabelConverter, labelStrings []*LabelConverter, labelFloats []*LabelConverter) (*Exporter, error) {
	le, err := NewLabelsExporter(namespace, labelBools, labelStrings, labelFloats)
	if err != nil {
		return nil, err
	}

	return &Exporter{
		scraper:  s,
		Counters: NewCounterContainer(namespace),
		Gauges:   NewGaugeContainer(namespace),
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Marathon.",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the last scrape of metrics from Marathon resulted in an error (0 for error, 1 for success).",
		}),
		scrapeError: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Marathon resulted in an error (1 for error, 0 for success).",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "scrapes_total",
			Help:      "Total number of times Marathon was scraped for metrics.",
		}),
		totalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "errors_total",
			Help:      "Total number of times the exporter experienced errors collecting Marathon metrics.",
		}),
		labelsExporter: le,
	}, nil
}
