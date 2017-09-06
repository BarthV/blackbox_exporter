package prober

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	minio "github.com/BarthV/minio-go"
	humanize "github.com/dustin/go-humanize"
	"github.com/prometheus/blackbox_exporter/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type s3Object struct {
	Bucket   string
	Filename string
	MD5      [md5.Size]byte
	ByteSize uint64
}

func timeListBuckets(c *minio.Client, r *prometheus.Registry) error {
	probeS3ListLatencyGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "probe_s3_list_latency_seconds",
		Help: "time waiting when listing every buckets (in seconds)",
	})
	r.MustRegister(probeS3ListLatencyGauge)

	log.Debugf("Listing S3 buckets")
	resolveStart := time.Now()
	_, err := c.ListBuckets()
	if err != nil {
		log.With("error", err).Errorf("Error Listing S3 bucket")
		return err
	}
	probeS3ListLatencyGauge.Add(time.Since(resolveStart).Seconds())
	return nil
}

func delObject(c *minio.Client, s3o s3Object) error {
	err := c.RemoveObject(s3o.Bucket, s3o.Filename)
	if err != nil {
		return err
	}
	return nil
}

func timeDelObject(c *minio.Client, r *prometheus.Registry, s3o s3Object) error {
	log.Debugf("Deleting a S3 Object")

	probeS3FileLatencyGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "probe_s3_file_latency_seconds",
		ConstLabels: prometheus.Labels{"action": "del", "bytesize": "-1"},
		Help:        "time waiting when working with S3 files (in seconds)",
	})
	r.MustRegister(probeS3FileLatencyGauge)

	resolveStart := time.Now()
	err := delObject(c, s3o)
	if err != nil {
		log.With("error", err).Errorf("Failed to remove S3 item : " + s3o.Filename)
		return err
	}

	probeS3FileLatencyGauge.Add(time.Since(resolveStart).Seconds())

	log.Debugf("Object Deleted : %s", s3o.Filename)
	return nil
}

func timeGetObject(c *minio.Client, r *prometheus.Registry, s3o s3Object) error {
	log.Debugf("Getting a %d bytes '%s' S3 file", s3o.ByteSize, s3o.Filename)

	probeS3FileLatencyGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "probe_s3_file_latency_seconds",
		ConstLabels: prometheus.Labels{"action": "get", "bytesize": fmt.Sprintf("%v", s3o.ByteSize)},
		Help:        "time waiting when working with S3 files (in seconds)",
	})
	r.MustRegister(probeS3FileLatencyGauge)

	resolveStart := time.Now()
	object, err := c.GetObject(s3o.Bucket, s3o.Filename)
	if err != nil {
		log.With("error", err).Errorf("Failed to get S3 file : %s", s3o.Filename)
		return err
	}
	defer object.Close()

	stat, err := object.Stat()
	if err != nil {
		log.With("error", err).Errorf("Error reading stats from S3 file")
		return err
	}

	buf := new(bytes.Buffer)
	defer buf.Reset()

	//TODO : byteSize : maybe move time reference here instead of before GetObject call
	if _, err := io.CopyN(buf, object, stat.Size); err != nil {
		log.With("error", err).Errorf("Error reading file from S3")
		return err
	}

	probeS3FileLatencyGauge.Add(time.Since(resolveStart).Seconds())

	getmd5sum := md5.Sum(buf.Bytes())
	log.With("s3Object", s3o).With("Size", stat.Size).Debug("Get file")

	if getmd5sum != s3o.MD5 {
		return fmt.Errorf("MD5sum mismatch : (set)%x - (get)%x", s3o.MD5, getmd5sum)
	}

	return nil
}

func timeSetObject(c *minio.Client, r *prometheus.Registry, s3o s3Object) (s3Object, error) {
	log.Debugf("Sending a %d bytes '%s' S3 file", s3o.ByteSize, s3o.Filename)

	probeS3FileLatencyGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "probe_s3_file_latency_seconds",
		ConstLabels: prometheus.Labels{"action": "set", "bytesize": fmt.Sprintf("%v", s3o.ByteSize)},
		Help:        "time waiting when working with S3 files (in seconds)",
	})
	r.MustRegister(probeS3FileLatencyGauge)

	buf := new(bytes.Buffer)
	defer buf.Reset()

	rndContent := make([]byte, s3o.ByteSize)
	rand.Read(rndContent)
	buf.Write(rndContent)

	resolveStart := time.Now()
	if _, err := c.PutObject(s3o.Bucket, s3o.Filename, buf, "application/octet-stream"); err != nil {
		log.With("error", err).Errorf("Error setting a file in S3 bucket")
		return s3o, err
	}
	probeS3FileLatencyGauge.Add(time.Since(resolveStart).Seconds())

	s3o.MD5 = md5.Sum(rndContent)
	log.Debugf("Set file MD5sum : %x", s3o.MD5)

	return s3o, nil
}

func timeSetGetObject(c *minio.Client, r *prometheus.Registry, s3o s3Object, w time.Duration) error {
	// Set object and fill prometheus registry with the result
	s3o, err := timeSetObject(c, r, s3o)
	if err != nil {
		return err
	}

	// Configurable wait before getting
	time.Sleep(w)

	// Get object and fill prometheus registry with the result
	if err := timeGetObject(c, r, s3o); err != nil {
		return err
	}

	return nil
}

type s3target struct {
	Bucket   string
	Endpoint string
}

func parseS3(target string) (s3target, error) {
	var s3t s3target
	u, err := url.Parse(target)
	if err != nil {
		log.With("error", err).Errorf("S3 target malformed")
		return s3t, err
	}
	if u.Scheme != "s3" {
		return s3t, errors.New("Specified target is not a S3 url")
	}
	s3t.Bucket = strings.TrimPrefix(u.Path, "/")
	s3t.Endpoint = u.Host
	return s3t, nil
}

func ProbeS3(ctx context.Context, target string, module config.Module, registry *prometheus.Registry) (success bool) {
	//         target string, w http.ResponseWriter, module Module, registry *prometheus.Registry
	// init local vars
	s3t, err := parseS3(target)
	if err != nil {
		return false
	}
	waitTimeAfterSet, err := time.ParseDuration(module.S3.WaitAfterSet)
	if err != nil {
		log.With("error", err).Errorf("Error parsing wait time after SET")
		return false
	}
	accessKeyID := module.S3.AccessKeyID
	secretAccessKey := module.S3.SecretAccessKey
	useSSL := module.S3.UseSSL
	signatureVersion := module.S3.SignatureVersion
	fileName := module.S3.FileName

	log.Debugf("Connecting to S3 endpoint '%s' using '%s' signature", s3t.Endpoint, signatureVersion)
	log.Debugf("Connecting using bucket '%s'", s3t.Bucket)

	// Spawning a S3 client
	var client *minio.Client
	switch signatureVersion {
	case "v2":
		client, err = minio.NewV2(s3t.Endpoint, accessKeyID, secretAccessKey, useSSL)
		if err != nil {
			log.With("error", err).Errorf("Error connecting to S3 endpoint")
			return false
		}
	case "v4":
		client, err = minio.NewV4(s3t.Endpoint, accessKeyID, secretAccessKey, useSSL)
		if err != nil {
			log.With("error", err).Errorf("Error connecting to S3 endpoint")
			return false
		}
	default:
		log.Errorf("Signature version not specified or not supported")
		return false
	}

	//TODO : support http proxy
	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 5 * time.Second,
		}).DialContext,
		//TODO : maybe remove IdleConnTimeout timeout
		IdleConnTimeout:       25 * time.Second,
		ResponseHeaderTimeout: module.Timeout,
	}

	client.SetCustomTransport(transport)

	// Listing buckets time
	if err := timeListBuckets(client, registry); err != nil {
		log.Errorf("Listing buckets failed, stopping s3 probe")
		return false
	}

	s3o := s3Object{
		Bucket:   s3t.Bucket,
		Filename: fmt.Sprintf("%v.%s", time.Now().UnixNano(), fileName),
	}
	defer delObject(client, s3o)

	// Probing S3 SET/GET with selected file sizes
	for _, fileSize := range module.S3.TestedFileSizes {
		byteSize, err := humanize.ParseBytes(fileSize)
		if err != nil {
			log.With("error", err).Errorf("Could not parse file size")
			return false
		}

		s3o.ByteSize = byteSize
		if err := timeSetGetObject(client, registry, s3o, waitTimeAfterSet); err != nil {
			log.With("error", err).Errorf("Set/Get sequence failed, stopping s3 probe")
			return false
		}
	}
	// Probing DEL of the unique probe file
	if err := timeDelObject(client, registry, s3o); err != nil {
		return false
	}
	return true
}
