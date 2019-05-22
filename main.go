package main

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/matt-deboer/go-marathon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type labelFlags []*LabelConverter

func (lf *labelFlags) Set(value string) error {
	// check if it's regexp=label
	content := strings.SplitN(value, "=", 2)
	if len(content) == 1 {
		// not a regexp/rename, just a simple name.
		content = append(content, value)
	}
	lc, err := NewLabelConverter(content[1], content[0])
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
	stringLabels labelFlags
	floatLabels  labelFlags
)

func init() {
	flag.Var(&stringLabels,
		"app.labels.string",
		"Which marathon string labels to export.  Either is the label name, or can be 'regexp=label' to support renaming of "+
			"labels on the fly, or deriving sub labels.  For example 'zoidberg_(?:port_(?P<port>\\d+))_app_name=zoidberg' would"+
			"match zoidberg_port_0_app_name, creating marathon_app_label_zoidberg{port='0'} 1",
	)
	flag.Var(&floatLabels,
		"app.labels.float",
		"Which marathon string labels to export the value as a float.  Either is the label name, or can be 'regexp=label' to support renaming of "+
			"labels on the fly, or deriving sub labels.  For example 'zoidberg_(?:port_(?P<port>\\d+))_app_name=zoidberg' would"+
			"match zoidberg_port_0_app_name, creating marathon_app_label_zoidberg{port='0'} <label_value>.  If the label value cannot be converted"+
			" to a float, the label value is dropped",
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

	exporter, err := NewExporter(&scraper{uri}, defaultNamespace, stringLabels, floatLabels)
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
