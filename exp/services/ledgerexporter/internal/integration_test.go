package ledgerexporter

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/suite"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/storage"
)

const (
	maxWaitForCoreStartup   = (30 * time.Second)
	coreStartupPingInterval = time.Second
)

func TestLedgerExporterTestSuite(t *testing.T) {
	if os.Getenv("LEDGEREXPORTER_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("skipping integration test: LEDGEREXPORTER_INTEGRATION_TESTS_ENABLED not true")
	}

	defineCommands()

	ledgerExporterSuite := &LedgerExporterTestSuite{}
	suite.Run(t, ledgerExporterSuite)
}

type LedgerExporterTestSuite struct {
	suite.Suite
	tempConfigFile  string
	ctx             context.Context
	ctxStop         context.CancelFunc
	coreContainerId string
	coreHttpPort    int
	dockerCli       *client.Client
	gcsServer       *fakestorage.Server
}

func (s *LedgerExporterTestSuite) TestScanAndFill() {
	require := s.Require()

	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", s.tempConfigFile})
	var errWriter io.Writer = &bytes.Buffer{}
	var outWriter io.Writer = &bytes.Buffer{}
	rootCmd.SetErr(errWriter)
	rootCmd.SetOut(outWriter)
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.(*bytes.Buffer).String()
	errOutput := errWriter.(*bytes.Buffer).String()
	s.T().Log(output)
	s.T().Log(errOutput)

	datastore, err := datastore.NewGCSDataStore(s.ctx, "integration-test/standalone")
	require.NoError(err)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr.zstd")
	require.NoError(err)
}

func (s *LedgerExporterTestSuite) TestAppend() {
	require := s.Require()

	// first populate ledgers 4-5
	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	// now run an append of overalapping range, it will resume past existing ledgers 4,5
	rootCmd.SetArgs([]string{"append", "--start", "4", "--end", "7", "--config-file", s.tempConfigFile})
	var errWriter io.Writer = &bytes.Buffer{}
	var outWriter io.Writer = &bytes.Buffer{}
	rootCmd.SetErr(errWriter)
	rootCmd.SetOut(outWriter)
	err = rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.(*bytes.Buffer).String()
	errOutput := errWriter.(*bytes.Buffer).String()
	s.T().Log(output)
	s.T().Log(errOutput)

	datastore, err := datastore.NewGCSDataStore(s.ctx, "integration-test/standalone")
	require.NoError(err)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFF8--7.xdr.zstd")
	require.NoError(err)
}

func (s *LedgerExporterTestSuite) SetupSuite() {
	var err error
	t := s.T()

	s.ctx, s.ctxStop = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	ledgerExporterConfigTemplate, err := toml.LoadFile("test/integration_config_template.toml")
	if err != nil {
		t.Fatalf("unable to load config template file %v", err)
	}

	// if LEDGEREXPORTER_INTEGRATION_TESTS_CAPTIVE_CORE_BIN specified,
	// ledgerexporter will attempt resolve core bin using 'stellar-core' from OS path
	ledgerExporterConfigTemplate.Set("stellar_core_config.stellar_core_binary_path",
		os.Getenv("LEDGEREXPORTER_INTEGRATION_TESTS_CAPTIVE_CORE_BIN"))

	tomlBytes, err := toml.Marshal(ledgerExporterConfigTemplate)
	if err != nil {
		t.Fatalf("unable to load config file %v", err)
	}
	testTempDir := t.TempDir()
	tempSeedDataPath := filepath.Join(testTempDir, "data")
	if err = os.MkdirAll(filepath.Join(tempSeedDataPath, "integration-test"), 0777); err != nil {
		t.Fatalf("unable to create seed data in temp path, %v", err)
	}

	s.tempConfigFile = filepath.Join(testTempDir, "config.toml")
	err = os.WriteFile(s.tempConfigFile, tomlBytes, 0777)
	if err != nil {
		t.Fatalf("unable to write temp config file %v, %v", s.tempConfigFile, err)
	}

	testWriter := &testWriter{test: t}
	opts := fakestorage.Options{
		Scheme:      "http",
		Host:        "127.0.0.1",
		Port:        uint16(0),
		Writer:      testWriter,
		Seed:        tempSeedDataPath,
		StorageRoot: filepath.Join(testTempDir, "bucket"),
		PublicHost:  "127.0.0.1",
	}

	s.gcsServer, err = fakestorage.NewServerWithOptions(opts)

	if err != nil {
		t.Fatalf("couldn't start the fake gcs http server %v", err)
	}

	t.Logf("fake gcs server started at %v", s.gcsServer.URL())
	t.Setenv("STORAGE_EMULATOR_HOST", s.gcsServer.URL())

	quickstartImage := os.Getenv("LEDGEREXPORTER_INTEGRATION_TESTS_QUICKSTART_IMAGE")
	if quickstartImage == "" {
		quickstartImage = "stellar/quickstart:testing"
	}
	s.mustStartCore(t, quickstartImage)
	s.mustWaitForCore(t, ledgerExporterConfigTemplate.GetArray("stellar_core_config.history_archive_urls").([]string),
		ledgerExporterConfigTemplate.Get("stellar_core_config.network_passphrase").(string))
}

func (s *LedgerExporterTestSuite) TearDownSuite() {
	if s.coreContainerId != "" {
		if err := s.dockerCli.ContainerStop(context.Background(), s.coreContainerId, container.StopOptions{}); err != nil {
			s.T().Logf("unable to stop core container, %v, %v", s.coreContainerId, err)
		}
		s.dockerCli.Close()
	}
	if s.gcsServer != nil {
		s.gcsServer.Stop()
	}
	s.ctxStop()
}

func (s *LedgerExporterTestSuite) mustStartCore(t *testing.T, quickstartImage string) {
	var err error
	s.dockerCli, err = client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("could not create docker client, %v", err)
	}

	img, err := s.dockerCli.ImagePull(s.ctx, quickstartImage, image.PullOptions{})
	if err != nil {
		t.Fatalf("could not pull docker image, %v, %v", quickstartImage, err)
	}
	img.Close()

	resp, err := s.dockerCli.ContainerCreate(s.ctx,
		&container.Config{
			Image:        quickstartImage,
			Cmd:          []string{"--enable", "core", "--local"},
			AttachStdout: true,
			AttachStderr: true,
			ExposedPorts: nat.PortSet{
				nat.Port("1570/tcp"):  {},
				nat.Port("11625/tcp"): {},
			},
		},

		&container.HostConfig{
			PortBindings: nat.PortMap{
				nat.Port("1570/tcp"):  {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "1570"}},
				nat.Port("11625/tcp"): {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "11625"}},
			},
			AutoRemove: true,
		},
		nil, nil, "")

	if err != nil {
		t.Fatalf("could not create quickstart docker container, %v, error %v", quickstartImage, err)
	}
	s.coreContainerId = resp.ID

	if err := s.dockerCli.ContainerStart(s.ctx, resp.ID, container.StartOptions{}); err != nil {
		t.Fatalf("could not run quickstart docker container, %v, error %v", quickstartImage, err)
	}
}

func (s *LedgerExporterTestSuite) mustWaitForCore(t *testing.T, archiveUrls []string, passphrase string) {
	t.Log("Waiting for core to be up...")
	//coreClient := &stellarcore.Client{URL: "http://localhost:" + strconv.Itoa(s.coreHttpPort)}
	startTime := time.Now()
	infoTime := startTime
	archive, err := historyarchive.NewArchivePool(archiveUrls, historyarchive.ArchiveOptions{
		NetworkPassphrase: passphrase,
		// due to ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING that is done by quickstart's local network
		CheckpointFrequency: 8,
		ConnectOptions: storage.ConnectOptions{
			Context: s.ctx,
		},
	})
	if err != nil {
		t.Fatalf("unable to create archive pool against core, %v", err)
	}
	for time.Since(startTime) < maxWaitForCoreStartup {
		if durationSince := time.Since(infoTime); durationSince < coreStartupPingInterval {
			time.Sleep(coreStartupPingInterval - durationSince)
		}
		infoTime = time.Now()
		has, requestErr := archive.GetRootHAS()
		if errors.Is(requestErr, context.Canceled) {
			break
		}
		if requestErr != nil {
			t.Logf("request to fetch checkpoint failed: %v", requestErr)
			continue
		}
		latestCheckpoint := has.CurrentLedger
		if latestCheckpoint > 1 {
			return
		}
	}
	t.Fatalf("core did not progress ledgers within %v seconds", maxWaitForCoreStartup)
}

type testWriter struct {
	test *testing.T
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.test.Log(string(p))
	return len(p), nil
}
