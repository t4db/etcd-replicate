// Command etcd-replicate continuously replicates an etcd v3 keyspace into
// another etcd v3 endpoint so that both are at identical revisions.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	replicate "github.com/t4db/etcd-replicate"
)

const envPrefix = "ETCD_REPLICATE_"

const usage = `Usage: etcd-replicate [flags]

Continuously replicate the source into the target, applying each source
revision as one target transaction so that both are at identical revisions.

The target must be empty (a fresh etcd cluster) or have been replicated into
before; its replication cursor lives under --state-prefix. The source must
spend revisions like etcd does. Only the replicator may write to the target.

Replication stops with an error instead of diverging: if the target changes
behind its back, the source compacts history the target still needs, or a
source revision exceeds the target's transaction limits (raise etcd's
--max-txn-ops and --max-request-bytes).

Every flag can also be set with an environment variable: ETCD_REPLICATE_ and
the flag name in upper case with - replaced by _, e.g.
ETCD_REPLICATE_SOURCE_ENDPOINTS.

Flags:
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "etcd-replicate:", err)
		os.Exit(1)
	}
}

// endpointFlags are the connection settings for one side of replication.
type endpointFlags struct {
	side      string
	endpoints string
	caCert    string
	cert      string
	key       string
	user      string
	password  string
}

func addEndpointFlags(fs *flag.FlagSet, side string) *endpointFlags {
	f := &endpointFlags{side: side}
	fs.StringVar(&f.endpoints, side+"-endpoints", "", fmt.Sprintf("comma-separated etcd v3 endpoints of the %s (required)", side))
	fs.StringVar(&f.caCert, side+"-cacert", "", fmt.Sprintf("CA bundle to verify the %s's TLS certificate", side))
	fs.StringVar(&f.cert, side+"-cert", "", fmt.Sprintf("client certificate for the %s", side))
	fs.StringVar(&f.key, side+"-key", "", fmt.Sprintf("client key for the %s", side))
	fs.StringVar(&f.user, side+"-user", "", fmt.Sprintf("username for the %s", side))
	fs.StringVar(&f.password, side+"-password", "", fmt.Sprintf("password for the %s", side))
	return f
}

func (f *endpointFlags) client() (*clientv3.Client, error) {
	if f.endpoints == "" {
		return nil, fmt.Errorf("--%s-endpoints is required", f.side)
	}
	cfg := clientv3.Config{
		Endpoints:   strings.Split(f.endpoints, ","),
		DialTimeout: 10 * time.Second,
		Username:    f.user,
		Password:    f.password,
	}
	if f.caCert != "" || f.cert != "" || f.key != "" {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if f.caCert != "" {
			pem, err := os.ReadFile(f.caCert)
			if err != nil {
				return nil, fmt.Errorf("--%s-cacert: %w", f.side, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("--%s-cacert: no certificates in %s", f.side, f.caCert)
			}
			tlsCfg.RootCAs = pool
		}
		if f.cert != "" || f.key != "" {
			cert, err := tls.LoadX509KeyPair(f.cert, f.key)
			if err != nil {
				return nil, fmt.Errorf("--%s-cert/--%s-key: %w", f.side, f.side, err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", f.side, err)
	}
	return cli, nil
}

// applyEnv sets every flag not given on the command line from its
// environment variable, if present.
func applyEnv(fs *flag.FlagSet) error {
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if set[f.Name] || err != nil {
			return
		}
		env := envPrefix + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		if v, ok := os.LookupEnv(env); ok {
			if e := fs.Set(f.Name, v); e != nil {
				err = fmt.Errorf("%s: %w", env, e)
			}
		}
	})
	return err
}

func run(args []string) error {
	fs := flag.NewFlagSet("etcd-replicate", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usage)
		fs.PrintDefaults()
	}
	source := addEndpointFlags(fs, "source")
	target := addEndpointFlags(fs, "target")
	statePrefix := fs.String("state-prefix", replicate.DefaultStatePrefix, "target key prefix for replication state; source keys under it are not replicated")
	leaseTTLMargin := fs.Duration("lease-ttl-margin", 10*time.Minute, "added to lease TTLs on the target so it never expires a lease before the source; must exceed the longest replicator downtime")
	reconcileInterval := fs.Duration("lease-reconcile-interval", 5*time.Second, "how often target leases are kept alive and leases gone from the source are revoked")
	metricsAddr := fs.String("metrics-addr", "0.0.0.0:9091", "HTTP address for /metrics and /healthz; empty disables")
	logLevel := fs.String("log-level", "info", "log level (debug/info/warn/error)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if err := applyEnv(fs); err != nil {
		return err
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid log level %q: %w", *logLevel, err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, err := source.client()
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := target.client()
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()

	reg := prometheus.NewRegistry()
	metrics := replicate.NewMetrics(reg)
	if *metricsAddr != "" {
		go serveMetrics(ctx, log, *metricsAddr, reg)
	}
	r, err := replicate.New(replicate.Config{
		Source:            src,
		Target:            dst,
		StatePrefix:       *statePrefix,
		LeaseTTLMargin:    *leaseTTLMargin,
		ReconcileInterval: *reconcileInterval,
		Log:               log,
		Metrics:           metrics,
	})
	if err != nil {
		return err
	}
	log.Info("replicate: starting", "source", source.endpoints, "target", target.endpoints)
	err = r.Run(ctx)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		log.Info("replicate: stopped", "revision", r.Applied())
		return nil
	}
	return err
}

func serveMetrics(ctx context.Context, log *slog.Logger, addr string, reg *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("replicate: metrics server", "err", err)
	}
}
