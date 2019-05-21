package main

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/matt-deboer/go-marathon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type labelFlags []string

func (lf *labelFlags) Set(value string) error {
	*lf = append(*lf, value)
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
)

func init() {
	flag.Var(&stringLabels,
		"app.labels.string",
		"Which marathon string labels to export")
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

	exporter, err := NewExporter(&scraper{uri}, defaultNamespace, stringLabels)
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
