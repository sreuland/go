package ledgerexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResumabilityDisabled(t *testing.T) {
	config := &Config{LedgerBatchConfig: LedgerBatchConfig{
		FilesPerPartition: uint32(1),
		LedgersPerFile:    uint32(10),
	}, Network: "testnet", Resume: false}

	mockDataStore := &MockDataStore{}
	mockNetworkManager := &MockNetworkManager{}
	ctx := context.Background()

	resumableManager := NewResumableManager(mockDataStore, config, mockNetworkManager)
	resumableStartLedger, dataStoreComplete := resumableManager.FindStartBoundary(ctx, 1, 10)
	require.Equal(t, uint32(0), resumableStartLedger)
	require.Equal(t, false, dataStoreComplete)
}

func TestResumability(t *testing.T) {

	tests := []struct {
		name                 string
		startLedger          uint32
		endLedger            uint32
		ledgerBatchConfig    LedgerBatchConfig
		resumableStartLedger uint32
		dataStoreComplete    bool
		networkName          string
	}{
		{
			name:                 "End ledger same as start, data store has it",
			startLedger:          4,
			endLedger:            4,
			resumableStartLedger: 0,
			dataStoreComplete:    true,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "End ledger same as start, data store does not have it",
			startLedger:          14,
			endLedger:            14,
			resumableStartLedger: 10,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "Data store is beyond boundary aligned start ledger",
			startLedger:          20,
			endLedger:            50,
			resumableStartLedger: 40,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "Data store is beyond non boundary aligned start ledger",
			startLedger:          55,
			endLedger:            85,
			resumableStartLedger: 80,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "Data store is beyond start and end ledger",
			startLedger:          255,
			endLedger:            275,
			resumableStartLedger: 0,
			dataStoreComplete:    true,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "Data store is not beyond start ledger",
			startLedger:          95,
			endLedger:            125,
			resumableStartLedger: 90,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "No start ledger provided",
			startLedger:          0,
			endLedger:            10,
			resumableStartLedger: 0,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test",
		},
		{
			name:                 "No end ledger provided, data store not beyond start",
			startLedger:          1145,
			endLedger:            0,
			resumableStartLedger: 1140,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test2",
		},
		{
			name:                 "No end ledger provided, data store is beyond start",
			startLedger:          2145,
			endLedger:            0,
			resumableStartLedger: 2250,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test3",
		},
		{
			name:                 "No end ledger provided, data store is beyond start and archive network latest, and partially into checkpoint frequency padding",
			startLedger:          3145,
			endLedger:            0,
			resumableStartLedger: 4070,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test4",
		},
		{
			name:                 "No end ledger provided, start is beyond archive network latest and checkpoint frequency padding",
			startLedger:          5129,
			endLedger:            0,
			resumableStartLedger: 0,
			dataStoreComplete:    false,
			ledgerBatchConfig: LedgerBatchConfig{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			networkName: "test5",
		},
	}

	ctx := context.Background()

	mockNetworkManager := &MockNetworkManager{}
	mockNetworkManager.On("GetLatestLedgerSequenceFromHistoryArchives", ctx, "test").Return(uint32(1000), nil)
	mockNetworkManager.On("GetLatestLedgerSequenceFromHistoryArchives", ctx, "test2").Return(uint32(2000), nil)
	mockNetworkManager.On("GetLatestLedgerSequenceFromHistoryArchives", ctx, "test3").Return(uint32(3000), nil)
	mockNetworkManager.On("GetLatestLedgerSequenceFromHistoryArchives", ctx, "test4").Return(uint32(4000), nil)
	mockNetworkManager.On("GetLatestLedgerSequenceFromHistoryArchives", ctx, "test5").Return(uint32(5000), nil)

	mockDataStore := &MockDataStore{}

	//"End ledger same as start, data store has it"
	mockDataStore.On("Exists", ctx, "0-9.xdr.gz").Return(true, nil).Once()

	//"End ledger same as start, data store does not have it"
	mockDataStore.On("Exists", ctx, "10-19.xdr.gz").Return(false, nil).Once()

	//"Data store is beyond boundary aligned start ledger"
	mockDataStore.On("Exists", ctx, "30-39.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "40-49.xdr.gz").Return(false, nil).Once()

	//"Data store is beyond non boundary aligned start ledger"
	mockDataStore.On("Exists", ctx, "70-79.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "80-89.xdr.gz").Return(false, nil).Once()

	//"Data store is beyond start and end ledger"
	mockDataStore.On("Exists", ctx, "260-269.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "270-279.xdr.gz").Return(true, nil).Once()

	//"Data store is not beyond start ledger"
	mockDataStore.On("Exists", ctx, "110-119.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "100-109.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "90-99.xdr.gz").Return(false, nil).Once()

	//"No end ledger provided, data store not beyond start" uses latest from network="test2"
	mockDataStore.On("Exists", ctx, "1630-1639.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1390-1399.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1260-1269.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1200-1209.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1160-1169.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1170-1179.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1150-1159.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "1140-1149.xdr.gz").Return(false, nil).Once()

	//"No end ledger provided, data store is beyond start" uses latest from network="test3"
	mockDataStore.On("Exists", ctx, "2630-2639.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "2390-2399.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "2260-2269.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "2250-2259.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "2240-2249.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "2230-2239.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "2200-2209.xdr.gz").Return(true, nil).Once()

	//"No end ledger provided, data store is beyond start and archive network latest, and partially into checkpoint frequency padding" uses latest from network="test4"
	mockDataStore.On("Exists", ctx, "3630-3639.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "3880-3889.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "4000-4009.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "4060-4069.xdr.gz").Return(true, nil).Once()
	mockDataStore.On("Exists", ctx, "4090-4099.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "4080-4089.xdr.gz").Return(false, nil).Once()
	mockDataStore.On("Exists", ctx, "4070-4079.xdr.gz").Return(false, nil).Once()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{LedgerBatchConfig: tt.ledgerBatchConfig, Network: tt.networkName, Resume: true}
			resumableManager := NewResumableManager(mockDataStore, config, mockNetworkManager)
			resumableStartLedger, dataStoreComplete := resumableManager.FindStartBoundary(ctx, tt.startLedger, tt.endLedger)
			require.Equal(t, tt.resumableStartLedger, resumableStartLedger)
			require.Equal(t, tt.dataStoreComplete, dataStoreComplete)
		})
	}

	mockDataStore.AssertExpectations(t)
}
