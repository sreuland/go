package ledgerbackend

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/support/log"
)

type runFromStream struct {
	dir            workingDir
	from           uint32
	fromHash       string
	runnerMode     stellarCoreRunnerMode
	coreCmdFactory coreCmdFactory
	log            *log.Entry
	useDB          bool
}

// all ledger tx meta emitted on pipe from this function will have trusted hashes, as it is built
// from core's online replay
// r - the core runner
// from - the ledger sequnce to start streaming additional ledgers there after
// fromHash - the hash of from ledger
// runnerMode - stellarCoreRunnerModePassive or stellarCoreRunnerModeActive
//
//	core toml settings, such as for offline, it will disable http port of core as it's not needed.
func newRunFromStream(r *stellarCoreRunner, from uint32, fromHash string, runnerMode stellarCoreRunnerMode) (runFromStream, error) {
	// Use ephemeral directories on windows because there is
	// no way to terminate captive core gracefully on windows.
	// Having an ephemeral directory ensures that it is wiped out
	// whenever captive core is terminated.
	dir, err := newWorkingDir(r, runnerMode == stellarCoreRunnerModePassive || runtime.GOOS == "windows")
	if err != nil {
		return runFromStream{}, err
	}
	return runFromStream{
		dir:            dir,
		from:           from,
		fromHash:       fromHash,
		runnerMode:     runnerMode,
		coreCmdFactory: newCoreCmdFactory(r, dir),
		log:            r.log,
		useDB:          r.useDB,
	}, nil
}

func (s runFromStream) getWorkingDir() workingDir {
	return s.dir
}

func (s runFromStream) offlineInfo(ctx context.Context) (stellarcore.InfoResponse, error) {
	cmd, err := s.coreCmdFactory.newCmd(ctx, stellarCoreRunnerModePassive, false, "offline-info")
	if err != nil {
		return stellarcore.InfoResponse{}, fmt.Errorf("error creating offline-info cmd: %w", err)
	}
	output, err := cmd.Output()
	if err != nil {
		return stellarcore.InfoResponse{}, fmt.Errorf("error executing offline-info cmd: %w", err)
	}
	var info stellarcore.InfoResponse
	err = json.Unmarshal(output, &info)
	if err != nil {
		return stellarcore.InfoResponse{}, fmt.Errorf("invalid output of offline-info cmd: %w", err)
	}
	return info, nil
}

func (s runFromStream) start(ctx context.Context) (cmd cmdI, captiveCorePipe pipe, returnErr error) {
	var err error
	var createNewDB bool
	defer func() {
		if returnErr != nil && createNewDB {
			// if we could not start captive core remove the new db we created
			s.dir.remove()
		}
	}()
	if s.useDB {
		// Check if on-disk core DB exists and what's the LCL there. If not what
		// we need remove storage dir and start from scratch.
		var info stellarcore.InfoResponse
		info, err = s.offlineInfo(ctx)
		if err != nil {
			s.log.Infof("Error running offline-info: %v, removing existing storage-dir contents", err)
			createNewDB = true
		} else if info.Info.Ledger.Num <= 1 || uint32(info.Info.Ledger.Num) > s.from {
			s.log.Infof("Unexpected LCL in Stellar-Core DB: %d (want: %d), removing existing storage-dir contents", info.Info.Ledger.Num, s.from)
			createNewDB = true
		}

		if createNewDB {
			if err = s.dir.remove(); err != nil {
				return nil, pipe{}, fmt.Errorf("error removing existing storage-dir contents: %w", err)
			}

			cmd, err = s.coreCmdFactory.newCmd(ctx, stellarCoreRunnerModePassive, true, "new-db")
			if err != nil {
				return nil, pipe{}, fmt.Errorf("error creating command: %w", err)
			}

			if err = cmd.Run(); err != nil {
				return nil, pipe{}, fmt.Errorf("error initializing core db: %w", err)
			}

			// This catchup is only run to set LCL on core's local storage to be our expected starting point.
			// No ledgers are emitted or collected from pipe during this execution.
			if s.from > 2 {
				cmd, err = s.coreCmdFactory.newCmd(ctx, stellarCoreRunnerModePassive, true,
					"catchup", "--force-untrusted-catchup", fmt.Sprintf("%d/0", s.from-1))

				if err != nil {
					return nil, pipe{}, fmt.Errorf("error creating catchup command to set LCL: %w", err)
				}

				if err = cmd.Run(); err != nil {
					return nil, pipe{}, fmt.Errorf("error running stellar-core catchup to set LCL: %w", err)
				}
			} else {
				// If the from is < 3, the caller wants ledger 2, to get that from core 'run'
				// we don't run catchup to set LCL, leave it at empty, new db state with LCL=1
				// and instead we set CATCHUP_COMPLETE=true, which will trigger core to emit ledger 2 first
				s.dir.toml.CatchupComplete = true
			}
		}

		// this will emit ledgers on the pipe, starting with sequence LCL+1
		cmd, err = s.coreCmdFactory.newCmd(ctx,
			s.runnerMode,
			true,
			"run",
			"--metadata-output-stream", s.coreCmdFactory.getPipeName(),
		)
	} else {
		// TODO, remove, this is effectively obsolete production code path, only tests reach this, production code path
		// only allows on-disk aka useDB mode.
		cmd, err = s.coreCmdFactory.newCmd(
			ctx,
			stellarCoreRunnerModeActive,
			true,
			"run",
			"--in-memory",
			"--start-at-ledger", fmt.Sprintf("%d", s.from),
			"--start-at-hash", s.fromHash,
			"--metadata-output-stream", s.coreCmdFactory.getPipeName(),
		)
	}
	if err != nil {
		return nil, pipe{}, fmt.Errorf("error creating command: %w", err)
	}

	captiveCorePipe, err = s.coreCmdFactory.startCaptiveCore(cmd)
	if err != nil {
		return nil, pipe{}, fmt.Errorf("error starting `stellar-core run` subprocess: %w", err)
	}

	return cmd, captiveCorePipe, nil
}
