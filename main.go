package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/matt-deboer/go-marathon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type defaultLabelFlags struct {
	labels DefaultLabels
}

func (dl *defaultLabelFlags) Set(value string) error {
	content := strings.Split(value, "=")
	if len(content) != 2 {
		return fmt.Errorf("default label ''%s' isn't in label=value format", value)
	}
	if dl.labels == nil {
		dl.labels = make(DefaultLabels)
	}
	if existing, ok := dl.labels[content[0]]; ok && existing != content[1] {
		return fmt.Errorf("default label '%s' tries to redefine existing default label value '%s' with %s", content[0], existing, content[1])
	}
	dl.labels[content[0]] = content[1]
	return nil
}

func (dl *defaultLabelFlags) String() string {
	return "default label"
}

type labelFlags []*LabelConverter

func (lf *labelFlags) Set(value string) error {
	// consider zoidberg_(port_(?P<port>\d+))?_app_name=zoidberg_app;port=0
	// that finds zoidberg_port_1_app_name and creates port="1" label.
	// That also finds zoidberg_app_name, and creates port="0" label
	content := strings.Split(value, ";")

	defaults := make(map[string]string, len(content)-1)
	// build defaults first.
	for _, chunk := range content[1:] {
		chunks := strings.Split(chunk, "=")
		if len(chunks) != 2 {
			return fmt.Errorf("defaults chunk '%s' isn't in key=value format", chunk)
		}
		defaults[strings.TrimSpace(chunks[0])] = strings.TrimSpace(chunks[1])
	}
	// process the label regexp next.
	content = strings.SplitN(strings.TrimSpace(content[0]), "=", 2)
	if len(content) == 1 {
		// not a regexp/rename, just a simple name.
		content = append(content, value)
	}
	lc, err := NewLabelConverter(strings.TrimSpace(content[1]), strings.TrimSpace(content[0]), defaults)
	if err != nil {
		return err
	}
	*lf = append(*lf, lc)
	return nil
}

func (lf *labelFlags) String() string {
	return "label flags"
}

var (
	listenAddress = flag.String(
		"web.listen-address", ":9088",
		"Address to listen on for web interface and telemetry.")

	metricsPath = flag.String(
		"web.telemetry-path", "/metrics",
		"Path under which to expose metrics.")

	marathonUri = flag.String(
		"marathon.uri", "http://marathon.mesos:8080",
		"URI of Marathon")
	boolLabels    labelFlags
	stringLabels  labelFlags
	floatLabels   labelFlags
	defaultLabels defaultLabelFlags
)

func init() {
	common := `

Format is regex[=rename][;label_key=label_default].  The first group is for matching and renaming metric- the following groups are used as defaults.

Consider: 'zoidberg(?:_port_(?P<port>\\d+))?_app_name = zoidberg ; port = 0'.  Given an app label of zoidberg_port_1_app_name,
this will result in metric 'marathon_app_label_zoidberg{port="1"}'.  Given an app label of zoidberg_app_name, the default port=0
kicks in and it results in 'marathon_app_label_zoidberg{port="0"}'.

Only named regex groups are supported; if the grouping is such that the value doesn't have to match, and no default matches that name, then '' is the label value.

Finally, note that spaces are not required between delimiters, just suggested for readability.
`

	flag.Var(&boolLabels,
		"app.labels.bool",
		"Export a constant gauge of 1 for any app that has a label that matches this.  The label value is dropped."+common,
	)
	flag.Var(&stringLabels,
		"app.labels.string",
		"Export a constant gauge of 1 for any app that has a label that matches, exporting the label value as a label named 'value'."+common,
	)
	flag.Var(&floatLabels,
		"app.labels.float",
		"Export the label value as a gauge for any app that has a label that matches, exporting the label value as a label named 'value'.\n"+
			"If the label value cannot be converted to a float, it's dropped instead."+common,
	)
	flag.Var(&defaultLabels,
		"app.labels.default",
		"For every app considered, default this marathon label to this value if it's not defined.  This should be used in combination \n"+
			"with -app.labels.* flags- this is a mechanism to provide defaults if the marathon label doesn't exist.",
	)
}

func marathonConnect(uri *url.URL) error {
	config := marathon.NewDefaultConfig()
	config.URL = uri.String()

	if uri.User != nil {
		if passwd, ok := uri.User.Password(); ok {
			config.HTTPBasicPassword = passwd
			config.HTTPBasicAuthUser = uri.User.Username()
		}
	}
	config.HTTPClient = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	log.Debugln("Connecting to Marathon")
	client, err := marathon.NewClient(config)
	if err != nil {
		return err
	}

	info, err := client.Info()
	if err != nil {
		return err
	}

	log.Debugf("Connected to Marathon! Name=%s, Version=%s\n", info.Name, info.Version)
	return nil
}

func main() {
	flag.Parse()
	uri, err := url.Parse(*marathonUri)
	if err != nil {
		log.Fatal(err)
	}

	retryTimeout := time.Duration(10 * time.Second)
	for {
		err := marathonConnect(uri)
		if err == nil {
			break
		}

		log.Debugf("Problem connecting to Marathon: %v", err)
		log.Infof("Couldn't connect to Marathon! Trying again in %v", retryTimeout)
		time.Sleep(retryTimeout)
	}

	exporter, err := NewExporter(&scraper{uri}, defaultNamespace, &(defaultLabels.labels), boolLabels, stringLabels, floatLabels)
	if err != nil {
		log.Fatalf("failed to create exporter: %s", err)
	}
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Marathon Exporter</title></head>
             <body>
             <h1>Marathon Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})

	log.Info("Starting Server: ", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
