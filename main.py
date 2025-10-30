from canton_scan_client import ScanApiClient

client = ScanApiClient("https://api.cantonnodes.com")

zzz = client.get_update_history_v1({
    "after_migration_id": 3,
    "after_record_time": "2025-10-26T02:34:33.000000Z",
},1000, "compact_json")