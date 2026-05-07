# DataFilter V5 — Integration Test Guide

* For old V4:
  * https://github.com/DUNE-DAQ/datafilter/tree/dunedaq-v4.1.1/
  * https://github.com/wchen2013a/dfbackend/tree/dunedaq-v4.1.1

---

## Quickstart

Setup the DataFilter first. 

```bash
wget https://raw.githubusercontent.com/DUNE-DAQ/datafilter/refs/heads/develop/setup-datafilter.sh
# The $INSTALL_DIR variable need to be defined before running the script.
chmod 755 setup-datafilter.sh
./setup-datafilter.sh /your/installation/path  
```
The script will build the project. Once it is done.

Open 4 terminals and run from `test/apps/` (start order does not matter):

```bash
cd test/apps   # in each terminal

# Terminal 1
trdispatcher

# Terminal 2
filterorchestrator

# Terminal 3
datafilter2

# Terminal 4
filterresultwriter
```

All configuration lives in `test/config/dfSession.data.xml`.

---

## Configuration Reference

### 1. TRDispatcher (`TRDispatcher_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `storage_pathname` | string | `/lcg/storage19/test-area/dune/trigger_records/sourcehdf5` | Directory containing source HDF5 files |
| `is_from_storage` | bool | `1` | `1` = read from HDF5 files; `0` = generate synthetic TRs |
| `input_h5_filename` | string | `np04hd_run024552_0011_...hdf5` | Single file to process; leave empty to cycle through all files in `json_file` |
| `json_file` | string | `hdf5_files_list.json` | JSON file tracking which source files have already been processed |
| `generate_trigger_record` | bool | `0` | `1` = generate synthetic TRs (ignores `is_from_storage` and HDF5 input) |
| `generate_time_slice` | bool | `0` | `1` = generate synthetic TSs (ignores `is_from_storage` and HDF5 input) |
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `1000` | Receive timeout in ms |

**Note on `hdf5_files_list.json`:** TRD skips any file already listed in this file.
Remove an entry before re-running to reprocess that file. The entry is re-added
automatically after a successful run. You can also add new HDF5 files, it will process automatically.

---

### 2. DataFilter (`DataFilter_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `adc_threshold` | u16 | `9145` | ADC threshold for TR filtering |

**ADC threshold semantics:** A trigger record is kept if any channel/sample in any WIBEth
fragment has a 14-bit ADC value `>= adc_threshold`. A TR is dropped only when **all** its
WIBEth fragments fail the threshold.

| Threshold range | Effect on `np04hd_run024552_0011` (28 TRs) |
|---|---|
| `≤ 9123` | All 28 TRs kept |
| `9130` | 27 kept, 1 dropped |
| `9145` | 20 kept, 8 dropped |
| `≥ 9170` | All 28 TRs dropped |

For fully saturated ADC data (e.g. `swtest_run001039`), max ADC = 16383; use a threshold
`> 16383` to drop all, or any value `≤ 16383` to keep all.

---

### 3. FilterResultWriter (`FilterResultWriter_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `odir` | string | `/lcg/storage18/dune/chen` | Output directory for filtered HDF5 files |
| `output_h5_filename` | string | `datafilter_output_h5_test` | Output filename prefix |
| `min_free_bytes` | u64 | `1024` | Minimum free disk space (bytes) required before FRW will write; set to `2147483648` (2 GB) for production |
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `1000` | Receive timeout in ms |

---

### 4. FilterOrchestrator (`FilterOrchestrator_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `0` | Receive timeout in ms (`0` = blocking) |

---

## Network Connection Port Map

All 15 `NetworkConnection` objects in `dfSession.data.xml`, with their ports, types, and roles:

| Connection ID | Port | Type | Direction | Purpose |
|---|---|---|---|---|
| `conn_A0_G0_C0_` | 15500 | kPubSub | TRD → DF | TriggerRecord data |
| `conn_A1_G0_C0_` | 15501 | kPubSub | DF → FRW | Filtered TriggerRecord data |
| `ts_conn_A0_G0_C0_` | 15510 | kPubSub | TRD → DF | TimeSlice data |
| `ts_conn_A1_G0_C0_` | 15511 | kPubSub | DF → FRW | Filtered TimeSlice data |
| `FO_ctrl0` | 12000 | kSendRecv | DF → FO | DF status feedback to FO |
| `TR_tracking0` | 13000 | kSendRecv | FRW ↔ TRD/FO | TR completion notifications (bidirectional) |
| `TR_tracking1` | 13001 | kSendRecv | → TRD | TRD handshake channel |
| `TR_tracking2` | 13002 | kSendRecv | → DF | DF handshake channel |
| `trdispatcher0` | 23000 | kSendRecv | FO → TRD | FO dispatch control to TRD |
| `trdispatcher1` | 23001 | kSendRecv | DF → FO | DF status to FO |
| `trwriter0` | 24000 | kSendRecv | DF → FRW | TR write command |
| `tswriter0` | 24001 | kSendRecv | DF → FRW | TS write command |
| `bookkeeping0` | 33000 | kSendRecv | TRD+FRW → DF | Bookkeeping messages to DF |
| `bookkeeping1` | 33001 | kSendRecv | DF → FRW | Initial BK metadata (run number, file attributes) |
| `bookkeeping2` | 33002 | kSendRecv | DF → TRD | Write confirmation (DF forwards FRW completion) |

---

## Multi-host IP Configuration

### Address format

Each `NetworkConnection` has an `address` attribute of the form:

```
tcp://<IP>:<PORT>
```

The default IP `127.0.0.1` works when all four apps run on the same host.
To split apps across servers, update the `address` on the relevant connections.

### Binding rules

| Connection type | Who binds | IP to use |
|---|---|---|
| `kSendRecv` | The **receiver** module binds | Use the receiver's host IP |
| `kPubSub` | The **publisher** module binds | Use the publisher's host IP |

### Example: 2-host deployment

**server1** (`10.0.0.14`): runs `trdispatcher`
**server2** (`10.0.0.13`): runs `filterorchestrator`, `datafilter2`, `filterresultwriter`

Update the `address` attribute of each `NetworkConnection` object in `dfSession.data.xml`:

| Connection ID | Port | Binder | New address |
|---|---|---|---|
| `conn_A0_G0_C0_` | 15500 | TRD (publisher) | `tcp://10.0.0.14:15500` |
| `ts_conn_A0_G0_C0_` | 15510 | TRD (publisher) | `tcp://10.0.0.14:15510` |
| `trdispatcher0` | 23000 | TRD (receiver) | `tcp://10.0.0.14:23000` |
| `TR_tracking1` | 13001 | TRD (receiver) | `tcp://10.0.0.14:13001` |
| `bookkeeping2` | 33002 | TRD (receiver) | `tcp://10.0.0.14:33002` |
| `conn_A1_G0_C0_` | 15501 | DF (publisher) | `tcp://10.0.0.13:15501` |
| `ts_conn_A1_G0_C0_` | 15511 | DF (publisher) | `tcp://10.0.0.13:15511` |
| `TR_tracking2` | 13002 | DF (receiver) | `tcp://10.0.0.13:13002` |
| `trdispatcher1` | 23001 | FO (receiver) | `tcp://10.0.0.13:23001` |
| `FO_ctrl0` | 12000 | FO (receiver) | `tcp://10.0.0.13:12000` |
| `TR_tracking0` | 13000 | FRW (receiver) | `tcp://10.0.0.13:13000` |
| `trwriter0` | 24000 | FRW (receiver) | `tcp://10.0.0.13:24000` |
| `tswriter0` | 24001 | FRW (receiver) | `tcp://10.0.0.13:24001` |
| `bookkeeping0` | 33000 | DF (receiver) | `tcp://10.0.0.13:33000` |
| `bookkeeping1` | 33001 | FRW (receiver) | `tcp://10.0.0.13:33001` |

**Firewall:** both servers must be able to reach each other on all 15 ports listed above.
