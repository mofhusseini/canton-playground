import requests
from typing import Optional, List, Dict, Any, Union, TypedDict, Tuple
from datetime import datetime, timezone
import csv
import sys

class ValidatorLicenseFaucetState(TypedDict):
    firstReceivedFor: Dict[str, str]
    lastReceivedFor: Dict[str, str]
    numCouponsMissed: str

class ValidatorLicenseMetadata(TypedDict):
    lastUpdatedAt: str
    version: str
    contactPoint: str

class ValidatorLicensePayload(TypedDict):
    dso: str
    sponsor: str
    lastActiveAt: str
    validator: str
    faucetState: ValidatorLicenseFaucetState
    metadata: ValidatorLicenseMetadata

class ValidatorLicense(TypedDict):
    template_id: str
    contract_id: str
    payload: ValidatorLicensePayload
    created_event_blob: str
    created_at: str

class RoundTotalEntry(TypedDict):
    closed_round: int
    closed_round_effective_at: str
    app_rewards: str
    validator_rewards: str
    change_to_initial_amount_as_of_round_zero: str
    change_to_holding_fees_rate: str
    cumulative_app_rewards: str
    cumulative_validator_rewards: str
    cumulative_change_to_initial_amount_as_of_round_zero: str
    cumulative_change_to_holding_fees_rate: str
    total_amulet_balance: str

class WalletBalanceResponse(TypedDict):
    wallet_balance: str

class RoundOfLatestDataResponse(TypedDict):
    round: int
    effectiveAt: str

class LatestWalletBalanceResponse(TypedDict):
    """Response for get_latest_wallet_balance containing the latest wallet balance with round and timestamp info."""
    round: int
    effective_at: str
    wallet_balance: str
    party_id: str

class WalletBalanceForMonth(TypedDict):
    party_id: str
    beginning_of_month_round: int
    beginning_of_month_time: Optional[str]
    beginning_of_month_balance: Optional[str]
    end_of_month_round: int
    end_of_month_time: Optional[str]
    end_of_month_balance: Optional[str]

class FindRoundsForMonthResult(TypedDict):
    start_round: int
    start_time: Optional[str]
    end_round: int
    end_time: Optional[str]

class HoldingsSummaryForMonth(TypedDict):
    party_id: str
    beginning_of_month_time: str
    beginning_of_month_balance: Optional[str]
    end_of_month_time: str
    end_of_month_balance: Optional[str]

class HoldingsSummaryEntry(TypedDict):
    party_id: str
    total_unlocked_coin: str
    total_locked_coin: str
    total_coin_holdings: str
    accumulated_holding_fees_unlocked: str
    accumulated_holding_fees_locked: str
    accumulated_holding_fees_total: str
    total_available_coin: str


class HoldingsSummaryResponse(TypedDict):
    record_time: str
    migration_id: int
    computed_as_of_round: int
    summaries: List[HoldingsSummaryEntry]

class ScanApiClient:
    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def readyz(self) -> Dict[str, Any]:
        """GET /readyz - Health check."""
        return self.session.get(f"{self.base_url}/readyz").json()

    def livez(self) -> Dict[str, Any]:
        """GET /livez - Liveness check."""
        return self.session.get(f"{self.base_url}/livez").json()

    def status(self) -> Dict[str, Any]:
        """GET /status - Status info."""
        return self.session.get(f"{self.base_url}/status").json()

    def version(self) -> Dict[str, Any]:
        """GET /version - Version info."""
        return self.session.get(f"{self.base_url}/version").json()

    def get_dso_info(self) -> Dict[str, Any]:
        """GET /v0/dso - DSO info."""
        return self.session.get(f"{self.base_url}/v0/dso").json()

    def get_validator_faucets_by_validator(self, validator_ids: List[str]) -> Dict[str, Any]:
        """
        GET /v0/validators/validator-faucets
        Input: validator_ids: List of validator party IDs.
        Output: JSON with validator faucet stats.
        """
        params = [("validator_ids", vid) for vid in validator_ids]
        return self.session.get(f"{self.base_url}/v0/validators/validator-faucets", params=params).json()

    def list_dso_scans(self) -> Dict[str, Any]:
        """GET /v0/scans - List DSO scans."""
        return self.session.get(f"{self.base_url}/v0/scans").json()

    def list_validator_licenses(self, after: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        GET /v0/admin/validator/licenses
        Input: after: next_page_token, limit: max elements.
        Output: JSON with validator licenses.
        """
        params = {}
        if after is not None:
            params["after"] = after
        if limit is not None:
            params["limit"] = limit
        return self.session.get(f"{self.base_url}/v0/admin/validator/licenses", params=params).json()

    def list_dso_sequencers(self) -> Dict[str, Any]:
        """GET /v0/dso-sequencers - List DSO sequencers."""
        return self.session.get(f"{self.base_url}/v0/dso-sequencers").json()

    def get_party_to_participant(self, domain_id: str, party_id: str) -> Dict[str, Any]:
        """
        GET /v0/domains/{domain_id}/parties/{party_id}/participant-id
        Input: domain_id, party_id
        Output: JSON with participant ID.
        """
        return self.session.get(f"{self.base_url}/v0/domains/{domain_id}/parties/{party_id}/participant-id").json()

    def get_member_traffic_status(self, domain_id: str, member_id: str) -> Dict[str, Any]:
        """
        GET /v0/domains/{domain_id}/members/{member_id}/traffic-status
        Input: domain_id, member_id
        Output: JSON with traffic status.
        """
        return self.session.get(f"{self.base_url}/v0/domains/{domain_id}/members/{member_id}/traffic-status").json()

    def get_closed_rounds(self) -> Dict[str, Any]:
        """GET /v0/closed-rounds - List closed mining rounds."""
        return self.session.get(f"{self.base_url}/v0/closed-rounds").json()

    def get_open_and_issuing_mining_rounds(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/open-and-issuing-mining-rounds
        Input: data: JSON body (contract IDs, etc.)
        Output: JSON with mining rounds.
        """
        return self.session.post(f"{self.base_url}/v0/open-and-issuing-mining-rounds", json=data).json()

    def get_update_history_v1(
        self,
        after: Optional[Dict[str, Any]] = None,
        page_size: Optional[int] = None,
        daml_value_encoding: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST /v1/updates
        Input:
            after: Optional dict with keys 'after_migration_id' (int), 'after_record_time' (str)
            page_size: Optional[int] - max number of results
            daml_value_encoding: Optional[str] - e.g. "compact_json"
        Output: JSON with update history.
        Example request body:
        {
            "after": {
                "after_migration_id": 1,
                "after_record_time": "string"
            },
            "page_size": 1,
            "daml_value_encoding": "compact_json"
        }
        """
        data: Dict[str, Any] = {}
        if after is not None:
            data["after"] = after
        if page_size is not None:
            data["page_size"] = page_size
        if daml_value_encoding is not None:
            data["daml_value_encoding"] = daml_value_encoding
        return self.session.post(f"{self.base_url}/v1/updates", json=data).json()

    def get_update_by_id_v1(self, update_id: str, daml_value_encoding: Optional[str] = None) -> Dict[str, Any]:
        """
        GET /v1/updates/{update_id}
        Input: update_id, daml_value_encoding (optional)
        Output: JSON with update info.
        """
        params = {}
        if daml_value_encoding:
            params["daml_value_encoding"] = daml_value_encoding
        return self.session.get(f"{self.base_url}/v1/updates/{update_id}", params=params).json()

    def get_acs_snapshot_timestamp(self, before: str, migration_id: int) -> Dict[str, Any]:
        """
        GET /v0/state/acs/snapshot-timestamp
        Input: before (str), migration_id (int)
        Output: JSON with snapshot timestamp.
        """
        params = {"before": before, "migration_id": migration_id}
        return self.session.get(f"{self.base_url}/v0/state/acs/snapshot-timestamp", params=params).json()

    def get_acs_snapshot_at(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/state/acs
        Input: data: JSON body (migration id, record time, etc.)
        Output: JSON with ACS snapshot.
        """
        return self.session.post(f"{self.base_url}/v0/state/acs", json=data).json()

    def force_acs_snapshot_now(self) -> Dict[str, Any]:
        """POST /v0/state/acs/force - Force ACS snapshot."""
        return self.session.post(f"{self.base_url}/v0/state/acs/force").json()

    def get_holdings_state_at(
        self,
        migration_id: int,
        record_time: Optional[str] = None,
        after: Optional[int] = None,
        page_size: Optional[int] = None,
        owner_party_ids: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        """
        POST /v0/holdings/state
        Input: migration_id (int), record_time (str, optional), after (int, optional), page_size (int, optional), owner_party_ids (list[str], optional)
        Output: JSON with holdings state.
        """
        data: Dict[str, Any] = {"migration_id": migration_id}
        if record_time is not None:
            data["record_time"] = record_time
        if after is not None:
            data["after"] = after
        if page_size is not None:
            data["page_size"] = page_size
        if owner_party_ids is not None:
            data["owner_party_ids"] = owner_party_ids
        return self.session.post(f"{self.base_url}/v0/holdings/state", json=data).json()

    def get_holdings_summary_at_time(
        self,
        migration_id: Optional[int] = None,
        record_time: Optional[str] = None,
        owner_party_ids: Optional[list[str]] = None,
        as_of_round: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        POST /v0/holdings/summary
        Input: migration_id (int), record_time (str, optional), owner_party_ids (list[str], optional), as_of_round (int, optional)
        Output: JSON with holdings summary.
        """
        data: Dict[str, Any] = {"migration_id": migration_id}
        if record_time is not None:
            data["record_time"] = record_time
        if owner_party_ids is not None:
            data["owner_party_ids"] = owner_party_ids
        if as_of_round is not None:
            data["as_of_round"] = as_of_round
        return self.session.post(f"{self.base_url}/v0/holdings/summary", json=data).json()

    def get_holdings_summary_at(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/holdings/summary
        Input: data: JSON body (migration id, record time, parties, etc.)
        Output: JSON with holdings summary.
        """
        return self.session.post(f"{self.base_url}/v0/holdings/summary", json=data).json()

    def list_ans_entries(self, name_prefix: Optional[str] = None, page_size: Optional[int] = None) -> Dict[str, Any]:
        """
        GET /v0/ans-entries
        Input: name_prefix (optional), page_size (optional)
        Output: JSON with ANS entries.
        """
        params = {}
        if name_prefix is not None:
            params["name_prefix"] = name_prefix
        if page_size is not None:
            params["page_size"] = page_size
        return self.session.get(f"{self.base_url}/v0/ans-entries", params=params).json()

    def lookup_ans_entry_by_party(self, party: str) -> Dict[str, Any]:
        """
        GET /v0/ans-entries/by-party/{party}
        Input: party (str)
        Output: JSON with ANS entry.
        """
        return self.session.get(f"{self.base_url}/v0/ans-entries/by-party/{party}").json()

    def lookup_ans_entry_by_name(self, name: str) -> Dict[str, Any]:
        """
        GET /v0/ans-entries/by-name/{name}
        Input: name (str)
        Output: JSON with ANS entry.
        """
        return self.session.get(f"{self.base_url}/v0/ans-entries/by-name/{name}").json()

    def get_dso_party_id(self) -> Dict[str, Any]:
        """GET /v0/dso-party-id - Get DSO party ID."""
        return self.session.get(f"{self.base_url}/v0/dso-party-id").json()

    def get_amulet_rules(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/amulet-rules
        Input: data: JSON body
        Output: JSON with amulet rules.
        """
        return self.session.post(f"{self.base_url}/v0/amulet-rules", json=data).json()

    def get_external_party_amulet_rules(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/external-party-amulet-rules
        Input: data: JSON body
        Output: JSON with external party amulet rules.
        """
        return self.session.post(f"{self.base_url}/v0/external-party-amulet-rules", json=data).json()

    def get_ans_rules(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/ans-rules
        Input: data: JSON body
        Output: JSON with ANS rules.
        """
        return self.session.post(f"{self.base_url}/v0/ans-rules", json=data).json()

    def list_featured_app_rights(self) -> Dict[str, Any]:
        """GET /v0/featured-apps - List featured app rights."""
        return self.session.get(f"{self.base_url}/v0/featured-apps").json()

    def lookup_featured_app_right(self, provider_party_id: str) -> Dict[str, Any]:
        """
        GET /v0/featured-apps/{provider_party_id}
        Input: provider_party_id (str)
        Output: JSON with featured app right.
        """
        return self.session.get(f"{self.base_url}/v0/featured-apps/{provider_party_id}").json()

    def get_top_validators_by_validator_faucets(self, limit: int) -> Dict[str, Any]:
        """
        GET /v0/top-validators-by-validator-faucets
        Input: limit (int)
        Output: JSON with top validators by faucets.
        """
        return self.session.get(f"{self.base_url}/v0/top-validators-by-validator-faucets", params={"limit": limit}).json()

    def lookup_transfer_preapproval_by_party(self, party: str) -> Dict[str, Any]:
        """
        GET /v0/transfer-preapprovals/by-party/{party}
        Input: party (str)
        Output: JSON with transfer preapproval.
        """
        return self.session.get(f"{self.base_url}/v0/transfer-preapprovals/by-party/{party}").json()

    def lookup_transfer_command_counter_by_party(self, party: str) -> Dict[str, Any]:
        """
        GET /v0/transfer-command-counter/{party}
        Input: party (str)
        Output: JSON with transfer command counter.
        """
        return self.session.get(f"{self.base_url}/v0/transfer-command-counter/{party}").json()

    def lookup_transfer_command_status(self, sender: str, nonce: str) -> Dict[str, Any]:
        """
        GET /v0/transfer-command/status
        Input: sender (str), nonce (str)
        Output: JSON with transfer command status.
        """
        params = {"sender": sender, "nonce": nonce}
        return self.session.get(f"{self.base_url}/v0/transfer-command/status", params=params).json()

    def get_migration_schedule(self) -> Dict[str, Any]:
        """GET /v0/migrations/schedule - Get migration schedule."""
        return self.session.get(f"{self.base_url}/v0/migrations/schedule").json()

    def get_synchronizer_identities(self, domain_id_prefix: str) -> Dict[str, Any]:
        """
        GET /v0/synchronizer-identities/{domain_id_prefix}
        Input: domain_id_prefix (str)
        Output: JSON with synchronizer identities.
        """
        return self.session.get(f"{self.base_url}/v0/synchronizer-identities/{domain_id_prefix}").json()

    def get_synchronizer_bootstrapping_transactions(self, domain_id_prefix: str) -> Dict[str, Any]:
        """
        GET /v0/synchronizer-bootstrapping-transactions/{domain_id_prefix}
        Input: domain_id_prefix (str)
        Output: JSON with bootstrapping transactions.
        """
        return self.session.get(f"{self.base_url}/v0/synchronizer-bootstrapping-transactions/{domain_id_prefix}").json()

    def get_splice_instance_names(self) -> Dict[str, Any]:
        """GET /v0/splice-instance-names - Get splice instance names."""
        return self.session.get(f"{self.base_url}/v0/splice-instance-names").json()

    def list_amulet_price_votes(self) -> Dict[str, Any]:
        """GET /v0/amulet-price/votes - List amulet price votes."""
        return self.session.get(f"{self.base_url}/v0/amulet-price/votes").json()

    def list_vote_requests_by_tracking_cid(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/voterequest
        Input: data: JSON body
        Output: JSON with vote requests.
        """
        return self.session.post(f"{self.base_url}/v0/voterequest", json=data).json()

    def lookup_dso_rules_vote_request(self, vote_request_contract_id: str) -> Dict[str, Any]:
        """
        GET /v0/voterequests/{vote_request_contract_id}
        Input: vote_request_contract_id (str)
        Output: JSON with DSO rules vote request.
        """
        return self.session.get(f"{self.base_url}/v0/voterequests/{vote_request_contract_id}").json()

    def list_dso_rules_vote_requests(self) -> Dict[str, Any]:
        """GET /v0/admin/sv/voterequests - List DSO rules vote requests."""
        return self.session.get(f"{self.base_url}/v0/admin/sv/voterequests").json()

    def list_vote_request_results(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/admin/sv/voteresults
        Input: data: JSON body
        Output: JSON with vote results.
        """
        return self.session.post(f"{self.base_url}/v0/admin/sv/voteresults", json=data).json()

    def get_migration_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/backfilling/migration-info
        Input: data: JSON body
        Output: JSON with migration info.
        """
        return self.session.post(f"{self.base_url}/v0/backfilling/migration-info", json=data).json()

    def get_updates_before(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/backfilling/updates-before
        Input: data: JSON body
        Output: JSON with updates before a given point.
        """
        return self.session.post(f"{self.base_url}/v0/backfilling/updates-before", json=data).json()

    # Deprecated endpoints (examples)
    def get_acs_snapshot(self, party: str) -> Dict[str, Any]:
        """
        GET /v0/acs/{party}
        Input: party (str)
        Output: JSON with ACS snapshot.
        """
        return self.session.get(f"{self.base_url}/v0/acs/{party}").json()

    def get_aggregated_rounds(self) -> Dict[str, Any]:
        """GET /v0/aggregated-rounds - Get aggregated rounds."""
        return self.session.get(f"{self.base_url}/v0/aggregated-rounds").json()

    def list_round_totals(self, start_round: int, end_round: int) -> list[RoundTotalEntry]:
        """
        POST /v0/round-totals
        Input: start_round (int), end_round (int)
        Output: List of round totals between start_round and end_round (inclusive).
        """
        data = {"start_round": start_round, "end_round": end_round}
        response = self.session.post(f"{self.base_url}/v0/round-totals", json=data).json()
        return response.get("entries", [])

    def list_round_party_totals(
        self,
        start_round: int,
        end_round: int,
    ) -> Dict[str, Any]:
        """
        POST /v0/round-party-totals
        Input:
            start_round: int - first round (inclusive)
            end_round: int - last round (inclusive)
        Example request body:
        {
            "start_round": 1,
            "end_round": 1
        }
        Output: JSON with round party totals.
        """
        data = {
            "start_round": start_round,
            "end_round": end_round,
        }
        return self.session.post(f"{self.base_url}/v0/round-party-totals", json=data).json()

    def get_total_amulet_balance(self, as_of_end_of_round: int) -> Dict[str, Any]:
        """
        GET /v0/total-amulet-balance
        Input: as_of_end_of_round (int)
        Output: JSON with total amulet balance.
        """
        return self.session.get(f"{self.base_url}/v0/total-amulet-balance", params={"asOfEndOfRound": as_of_end_of_round}).json()

    def get_wallet_balance(self, party_id: str, as_of_end_of_round: int) -> WalletBalanceResponse:
        """
        GET /v0/wallet-balance
        Input: party_id (str), as_of_end_of_round (int)
        Output: JSON with wallet balance.
        """
        params = {"party_id": party_id, "asOfEndOfRound": as_of_end_of_round}
        return self.session.get(f"{self.base_url}/v0/wallet-balance", params=params).json()

    def get_latest_wallet_balance(self, party_id: str) -> LatestWalletBalanceResponse:
        """
        Gets the latest wallet balance for a party by:
        1. Getting the latest round number from get_round_of_latest_data
        2. Getting the wallet balance for that round
        
        Returns: Dict with keys:
            - round: The latest round number
            - effective_at: ISO8601 timestamp when the round was effective
            - wallet_balance: The wallet balance as a string
        """
        # Get the latest round data
        latest_round_data = self.get_round_of_latest_data()
        latest_round = latest_round_data.get("round")
        effective_at = latest_round_data.get("effectiveAt")
        
        if latest_round is None:
            raise ValueError("Could not determine latest round")
            
        # Get the wallet balance for the latest round
        balance_data = self.get_wallet_balance(party_id, latest_round)
        wallet_balance = balance_data.get("wallet_balance")
        
        return {
            "round": latest_round,
            "effective_at": effective_at,
            "wallet_balance": wallet_balance,
            "party_id": party_id
        }

    def get_amulet_config_for_round(self, round_num: int) -> Dict[str, Any]:
        """
        GET /v0/amulet-config-for-round
        Input: round_num (int)
        Output: JSON with amulet config.
        """
        return self.session.get(f"{self.base_url}/v0/amulet-config-for-round", params={"round": round_num}).json()

    def get_round_of_latest_data(self) -> RoundOfLatestDataResponse:
        """GET /v0/round-of-latest-data - Get round of latest data."""
        return self.session.get(f"{self.base_url}/v0/round-of-latest-data").json()

    def get_rewards_collected(self, round_num: Optional[int] = None) -> Dict[str, Any]:
        """
        GET /v0/rewards-collected
        Input: round_num (optional int)
        Output: JSON with rewards collected.
        """
        params = {}
        if round_num is not None:
            params["round"] = round_num
        return self.session.get(f"{self.base_url}/v0/rewards-collected", params=params).json()

    def get_top_providers_by_app_rewards(self, round_num: int, limit: int) -> Dict[str, Any]:
        """
        GET /v0/top-providers-by-app-rewards
        Input: round_num (int), limit (int)
        Output: JSON with top providers by app rewards.
        """
        params = {"round": round_num, "limit": limit}
        return self.session.get(f"{self.base_url}/v0/top-providers-by-app-rewards", params=params).json()

    def get_top_validators_by_validator_rewards(self, round_num: int, limit: int) -> Dict[str, Any]:
        """
        GET /v0/top-validators-by-validator-rewards
        Input: round_num (int), limit (int)
        Output: JSON with top validators by validator rewards.
        """
        params = {"round": round_num, "limit": limit}
        return self.session.get(f"{self.base_url}/v0/top-validators-by-validator-rewards", params=params).json()

    def get_top_validators_by_purchased_traffic(self, round_num: int, limit: int) -> Dict[str, Any]:
        """
        GET /v0/top-validators-by-purchased-traffic
        Input: round_num (int), limit (int)
        Output: JSON with top validators by purchased traffic.
        """
        params = {"round": round_num, "limit": limit}
        return self.session.get(f"{self.base_url}/v0/top-validators-by-purchased-traffic", params=params).json()

    def list_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/activities
        Input: data: JSON body
        Output: JSON with activities.
        """
        return self.session.post(f"{self.base_url}/v0/activities", json=data).json()

    def list_transaction_history(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/transactions
        Input: data: JSON body
        Output: JSON with transaction history.
        """
        return self.session.post(f"{self.base_url}/v0/transactions", json=data).json()

    def get_update_history(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v0/updates
        Input: data: JSON body
        Output: JSON with update history.
        """
        return self.session.post(f"{self.base_url}/v0/updates", json=data).json()

    def get_update_by_id(self, update_id: str, lossless: Optional[bool] = None) -> Dict[str, Any]:
        """
        GET /v0/updates/{update_id}
        Input: update_id (str), lossless (optional bool)
        Output: JSON with update info.
        """
        params = {}
        if lossless is not None:
            params["lossless"] = lossless
        return self.session.get(f"{self.base_url}/v0/updates/{update_id}", params=params).json()

    def feature_support(self) -> Dict[str, Any]:
        """GET /v0/feature-support - Get feature support info."""
        return self.session.get(f"{self.base_url}/v0/feature-support").json()

    def find_rounds_for_month(self, year: int, month: int) -> Optional[FindRoundsForMonthResult]:
        """
        Find the start and end round numbers and their effective times for a specific month and year using binary search.
        Returns a dict with start_round, start_time, end_round, end_time or None if not found.
        """
        latest_data = self.get_round_of_latest_data()
        latest_round = latest_data.get("round")
        if latest_round is None:
            return None

        batch_size = 200

        def get_effective_at(round_num: int) -> Optional[str]:
            batch_start = (round_num // batch_size) * batch_size
            batch_end = min(batch_start + batch_size - 1, latest_round)
            entries = self.list_round_totals(batch_start, batch_end)
            for entry in entries:
                effective_at = (
                    entry.get("closed_round_effective_at")
                    or entry.get("effectiveAt")
                    or entry.get("effective_at")
                )
                if entry.get("closed_round") == round_num and effective_at:
                    return effective_at
            return None

        def get_effective_at_dt(round_num: int) -> Optional[datetime]:
            effective_at = get_effective_at(round_num)
            if effective_at:
                try:
                    return datetime.fromisoformat(effective_at.replace("Z", "+00:00"))
                except Exception:
                    return None
            return None

        # Binary search for the first round in the month
        left, right = 0, latest_round
        first_in_month = None
        while left <= right:
            mid = (left + right) // 2
            dt = get_effective_at_dt(mid)
            if dt is None:
                left = mid + 1
                continue
            if (dt.year, dt.month) < (year, month):
                left = mid + 1
            elif (dt.year, dt.month) > (year, month):
                right = mid - 1
            else:
                first_in_month = mid
                right = mid - 1  # search for earlier in month

        if first_in_month is None:
            return None

        # Binary search for the last round in the month
        left, right = first_in_month, latest_round
        last_in_month = first_in_month
        while left <= right:
            mid = (left + right) // 2
            dt = get_effective_at_dt(mid)
            if dt is None:
                left = mid + 1
                continue
            if (dt.year, dt.month) > (year, month):
                right = mid - 1
            elif (dt.year, dt.month) < (year, month):
                left = mid + 1
            else:
                last_in_month = mid
                left = mid + 1  # search for later in month

        # Get the effective times for the found rounds
        start_time = get_effective_at(first_in_month)
        end_time = get_effective_at(last_in_month)

        return {
            "start_round": first_in_month,
            "start_time": start_time,
            "end_round": last_in_month,
            "end_time": end_time,
        }

    def get_wallet_balances_for_month_from_rounds_estimate(
        self, party_ids: list[str], year: int, month: int, csv_path: Optional[str] = None
    ) -> list[WalletBalanceForMonth]:
        """
        For each party_id, get wallet balance at the beginning and end of the given month.
        If csv_path is provided, write the results to a CSV file.
        Returns a list of WalletBalanceForMonth dicts.
        """
        result: list[WalletBalanceForMonth] = []
        rounds = self.find_rounds_for_month(year, month)
        if not rounds:
            return result
        start_round = rounds["start_round"]
        end_round = rounds["end_round"]
        start_time = rounds["start_time"]
        end_time = rounds["end_time"]

        for party_id in party_ids:
            begin_balance = self.get_wallet_balance(party_id, start_round).get("wallet_balance")
            end_balance = self.get_wallet_balance(party_id, end_round).get("wallet_balance")
            result.append({
                "party_id": party_id,
                "beginning_of_month_round": start_round,
                "beginning_of_month_time": start_time,
                "beginning_of_month_balance": begin_balance,
                "end_of_month_round": end_round,
                "end_of_month_time": end_time,
                "end_of_month_balance": end_balance,
            })

        if csv_path:
            fieldnames = [
                "party_id",
                "beginning_of_month_round",
                "beginning_of_month_time",
                "beginning_of_month_balance",
                "end_of_month_round",
                "end_of_month_time",
                "end_of_month_balance",
            ]
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in result:
                    writer.writerow(row)

        return result

    def get_wallet_balances_for_all_rounds_in_month(
        self,
        party_id: str,
        year: int,
        month: int,
        csv: bool = False,
        step: int = 1,
    ) -> list[dict]:
        """
        For a given party_id and (year, month), return a list of dicts with wallet balances for every round in that month.
        Each dict contains: round, effective_time, wallet_balance.
        If csv=True, also writes the results to a CSV file named 'wallet_balances_{party_id_short}_{year}_{month:02d}.csv'.
        If step > 1, only include every 'step' rounds.
        """
        import sys
        print(f"[INFO] Finding rounds for {year}-{month:02d}...", file=sys.stderr)
        rounds_info = self.find_rounds_for_month(year, month)
        if not rounds_info:
            print(f"[WARN] No rounds found for {year}-{month:02d}", file=sys.stderr)
            return []
        start_round = rounds_info["start_round"]
        end_round = rounds_info["end_round"]
        print(f"[DEBUG] start_round={start_round}, end_round={end_round}", file=sys.stderr)

        # Get effective times for all rounds in the month
        round_to_effective_time = {}
        batch_size = 200
        for batch_start in range(start_round, end_round + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_round)
            print(f"[DEBUG] Fetching round totals for rounds {batch_start} to {batch_end}", file=sys.stderr)
            for entry in self.list_round_totals(batch_start, batch_end):
                round_num = entry.get("closed_round")
                eff_time = (
                    entry.get("closed_round_effective_at")
                    or entry.get("effectiveAt")
                    or entry.get("effective_at")
                )
                if round_num is not None and eff_time:
                    round_to_effective_time[round_num] = eff_time

        results = []
        print(f"[INFO] Fetching wallet balances for party_id={party_id} every {step} rounds", file=sys.stderr)
        for r in range(start_round, end_round + 1, step):
            eff_time = round_to_effective_time.get(r)
            balance_resp = self.get_wallet_balance(party_id, r)
            wallet_balance = balance_resp.get("wallet_balance")
            print(f"[DEBUG] round={r}, effective_time={eff_time}, wallet_balance={wallet_balance}", file=sys.stderr)
            results.append({
                "round": r,
                "effective_time": eff_time,
                "wallet_balance": wallet_balance,
            })

        if csv:
            import csv as _csv
            party_short = party_id.split("::")[0]
            filename = f"wallet_balances_{party_short}_{year}_{month:02d}.csv"
            print(f"[INFO] Writing results to {filename}", file=sys.stderr)
            with open(filename, "w", newline="") as f:
                writer = _csv.DictWriter(f, fieldnames=["round", "effective_time", "wallet_balance"])
                writer.writeheader()
                for row in results:
                    writer.writerow(row)

        print(f"[INFO] Done get_wallet_balances_for_all_rounds_in_month for {year}-{month:02d}", file=sys.stderr)
        return results

    def get_wallet_balances_for_rounds(
        self,
        party_id: str,
        first_round: int,
        last_round: int,
        step: int = 1,
        csv: bool = False,
    ) -> list[dict]:
        """
        For a given party_id and a range of rounds, return a list of dicts with wallet balances for each round in [first_round, last_round] (inclusive).
        Each dict contains: round, effective_time, wallet_balance.
        If step > 1, only include every 'step' rounds.
        If csv=True, writes the results to a CSV file named 'wallet_balances_{party_id_short}_{first_round}_{last_round}.csv'.
        """
        import sys
        print(f"[INFO] Fetching wallet balances for party_id={party_id} from round {first_round} to {last_round} every {step} rounds", file=sys.stderr)

        # Get effective times for all rounds in the range
        round_to_effective_time = {}
        batch_size = 200
        for batch_start in range(first_round, last_round + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, last_round)
            print(f"[DEBUG] Fetching round totals for rounds {batch_start} to {batch_end}", file=sys.stderr)
            for entry in self.list_round_totals(batch_start, batch_end):
                round_num = entry.get("closed_round")
                eff_time = (
                    entry.get("closed_round_effective_at")
                    or entry.get("effectiveAt")
                    or entry.get("effective_at")
                )
                if round_num is not None and eff_time:
                    round_to_effective_time[round_num] = eff_time

        results = []
        for r in range(first_round, last_round + 1, step):
            eff_time = round_to_effective_time.get(r)
            balance_resp = self.get_wallet_balance(party_id, r)
            wallet_balance = balance_resp.get("wallet_balance")
            print(f"[DEBUG] round={r}, effective_time={eff_time}, wallet_balance={wallet_balance}", file=sys.stderr)
            results.append({
                "round": r,
                "effective_time": eff_time,
                "wallet_balance": wallet_balance,
            })

        if csv:
            import csv as _csv
            party_short = party_id.split("::")[0]
            filename = f"wallet_balances_{party_short}_{first_round}_{last_round}.csv"
            print(f"[INFO] Writing results to {filename}", file=sys.stderr)
            with open(filename, "w", newline="") as f:
                writer = _csv.DictWriter(f, fieldnames=["round", "effective_time", "wallet_balance"])
                writer.writeheader()
                for row in results:
                    writer.writerow(row)

        print(f"[INFO] Done get_wallet_balances_for_rounds for {party_id} from {first_round} to {last_round}", file=sys.stderr)
        return results

    def get_holdings_summary_for_month(
        self,
        migration_id: int,
        party_ids: list[str],
        months: list[tuple[int, int]],
        csv_path: Optional[str] = None,
    ) -> list[HoldingsSummaryForMonth]:
        """
        For each party_id, get holdings summary (total_coin_holdings) at the beginning and end of each (year, month) in months.
        months: list of (year, month) tuples.
        If csv_path is provided, write the results to a CSV file (without time columns).
        Returns a list of HoldingsSummaryForMonth dicts.
        """
        import sys

        # Helper to process a single month
        def process_month(year: int, month: int) -> list[HoldingsSummaryForMonth]:
            print(f"[INFO] Processing holdings summary for {year}-{month:02d}...", file=sys.stderr)
            # Format beginning of month timestamp
            begin_time = f"{year:04d}-{month:02d}-01T00:00:00.000000Z"

            # Format end of month timestamp (use 23:59:00 for the last day)
            from calendar import monthrange
            last_day = monthrange(year, month)[1]
            end_time_query = f"{year:04d}-{month:02d}-{last_day:02d}T23:59:59.000000Z"

            # Get the actual record_time for the beginning and end of month
            print(f"[DEBUG] Getting ACS snapshot timestamp for begin_time_query={begin_time} migration_id={migration_id}", file=sys.stderr)
            acs_snapshot_begin = self.get_acs_snapshot_timestamp(begin_time, migration_id)
            begin_time_snapshot = acs_snapshot_begin.get("record_time")
            print(f"[DEBUG] begin_time_snapshot for {year}-{month:02d} is {begin_time_snapshot}", file=sys.stderr)

            print(f"[DEBUG] Getting ACS snapshot timestamp for end_time_query={end_time_query} migration_id={migration_id}", file=sys.stderr)
            acs_snapshot_end = self.get_acs_snapshot_timestamp(end_time_query, migration_id)
            end_time_snapshot = acs_snapshot_end.get("record_time")
            print(f"[DEBUG] end_time_snapshot for {year}-{month:02d} is {end_time_snapshot}", file=sys.stderr)
            if not end_time_snapshot:
                print(f"[WARN] No end_time found for {year}-{month:02d}", file=sys.stderr)
                return []

            # Call holdings summary at beginning and end
            print(f"[DEBUG] Getting holdings summary at begin_time={begin_time}", file=sys.stderr)
            begin_summary = self.get_holdings_summary_at_time(migration_id, begin_time, party_ids)
            print(f"[DEBUG] Getting holdings summary at end_time={end_time_snapshot}", file=sys.stderr)
            end_summary = self.get_holdings_summary_at_time(migration_id, end_time_snapshot, party_ids)

            def extract_holdings(summary: dict) -> dict[str, Optional[str]]:
                result = {}
                for s in summary.get("summaries", []):
                    result[s["party_id"]] = s.get("total_coin_holdings")
                return result

            begin_balances = extract_holdings(begin_summary)
            end_balances = extract_holdings(end_summary)

            result: list[HoldingsSummaryForMonth] = []
            for party_id in party_ids:
                result.append({
                    "party_id": party_id,
                    "beginning_of_month_time": begin_time,
                    "beginning_of_month_snapshot_time": begin_time_snapshot,
                    "beginning_of_month_balance": begin_balances.get(party_id),
                    "end_of_month_time": end_time_query,
                    "end_of_month_snapshot_time": end_time_snapshot,
                    "end_of_month_balance": end_balances.get(party_id),
                    "year": year,
                    "month": month,
                })
            print(f"[INFO] Finished holdings summary for {year}-{month:02d}", file=sys.stderr)
            return result

        all_results: list[HoldingsSummaryForMonth] = []
        for (y, m) in months:
            all_results.extend(process_month(y, m))

        if csv_path:
            fieldnames = [
                "party_id",
                "year",
                "month",
                "beginning_of_month_time",
                "beginning_of_month_snapshot_time",
                "beginning_of_month_balance",
                "end_of_month_time",
                "end_of_month_snapshot_time",
                "end_of_month_balance",
            ]
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in all_results:
                    writer.writerow({
                        "party_id": row["party_id"],
                        "year": row["year"],
                        "month": row["month"],
                        "beginning_of_month_time": row["beginning_of_month_time"],
                        "beginning_of_month_snapshot_time": row["beginning_of_month_snapshot_time"],
                        "beginning_of_month_balance": row["beginning_of_month_balance"],
                        "end_of_month_time": row["end_of_month_time"],
                        "end_of_month_snapshot_time": row["end_of_month_snapshot_time"],
                        "end_of_month_balance": row["end_of_month_balance"],
                    })
            print(f"[INFO] Wrote holdings summary for {len(months)} month(s) to {csv_path}", file=sys.stderr)

        return all_results

    def export_activities_after(
        self,
        after_timestamp: str,
        csv_path: str,
        page_size: int = 1000,
    ) -> None:
        """
        Streams all activities after the given ISO8601 timestamp (exclusive) to a CSV file.
        Stops when an activity with date < after_timestamp is encountered.
        Writes all fields present in the activity dict, flattening nested fields (e.g. transfer_provider, transfer_sender_party).
        Dynamically determines the max number of receivers and balance_changes and creates columns accordingly.
        Logs progress to stdout.
        """
        import json

        def flatten(d, max_receivers=0, max_balance_changes=0):
            """Flattens a dict, expanding transfer.receivers and transfer.balance_changes up to max counts."""
            items = {}
            for k, v in d.items():
                if k == "transfer" and isinstance(v, dict):
                    # Flatten transfer fields
                    for tk, tv in v.items():
                        if tk == "receivers" and isinstance(tv, list):
                            for i in range(max_receivers):
                                if i < len(tv) and isinstance(tv[i], dict):
                                    for rk, rv in tv[i].items():
                                        items[f"transfer_receivers_{i}_{rk}"] = rv
                                else:
                                    # Fill empty columns if not enough receivers
                                    for rk in ["party", "amount", "receiver_fee"]:
                                        items[f"transfer_receivers_{i}_{rk}"] = None
                            # Also store the full list as JSON
                            items["transfer_receivers"] = json.dumps(tv)
                        elif tk == "balance_changes" and isinstance(tv, list):
                            for i in range(max_balance_changes):
                                if i < len(tv) and isinstance(tv[i], dict):
                                    for bk, bv in tv[i].items():
                                        items[f"transfer_balance_changes_{i}_{bk}"] = bv
                                else:
                                    for bk in ["party", "change_to_initial_amount_as_of_round_zero", "change_to_holding_fees_rate"]:
                                        items[f"transfer_balance_changes_{i}_{bk}"] = None
                            items["transfer_balance_changes"] = json.dumps(tv)
                        elif isinstance(tv, dict):
                            for sk, sv in tv.items():
                                items[f"transfer_{tk}_{sk}"] = sv
                        else:
                            items[f"transfer_{tk}"] = tv
                elif isinstance(v, dict):
                    for sk, sv in v.items():
                        items[f"{k}_{sk}"] = sv
                elif isinstance(v, list):
                    items[k] = json.dumps(v)
                else:
                    items[k] = v
            return items

        # First pass: scan all activities to determine max receivers and balance_changes
        begin_after_id = ""
        total_scanned = 0
        max_receivers = 0
        max_balance_changes = 0
        all_flattened = []
        print("Scanning activities to determine max receivers and balance_changes...")
        batch_num = 0
        while True:
            resp = self.list_activity({"page_size": page_size, "begin_after_id": begin_after_id})
            activities = resp.get("activities", [])
            if not activities:
                print(f"No more activities after batch {batch_num}.")
                break
            stopped = False
            print(f"Processing batch {batch_num}, activities in batch: {len(activities)}, total scanned so far: {total_scanned}")
            for idx, activity in enumerate(activities):
                if idx > 0 and idx % 1000 == 0:
                    print(f"  ...processed {idx} activities in current batch...")
                activity_date = activity.get("date")
                if activity_date and activity_date < after_timestamp:
                    print(f"Stopping scan at activity_date={activity_date} (before after_timestamp={after_timestamp})")
                    print(f"Total scanned before stop: {total_scanned}")
                    print(f"Activity: {activity}")
                    stopped = True
                    break
                transfer = activity.get("transfer")
                if transfer:
                    receivers = transfer.get("receivers", [])
                    balance_changes = transfer.get("balance_changes", [])
                    max_receivers = max(max_receivers, len(receivers))
                    max_balance_changes = max(max_balance_changes, len(balance_changes))
                all_flattened.append(activity)
                total_scanned += 1
            if stopped:
                print(f"Stopped scanning batch {batch_num} at activity_date={activity_date}")
                break
            begin_after_id = activities[-1].get("event_id") if activities else None
            batch_num += 1
            if not begin_after_id or (activities and activities[-1].get("date") and activities[-1].get("date") < after_timestamp):
                if activities and activities[-1].get("date") and activities[-1].get("date") < after_timestamp:
                    print(f"Breaking batch loop at last activity_date={activities[-1].get('date')} (before after_timestamp={after_timestamp})")
                print("breaking batch loop at last activity_date")
                break
        print(f"Max receivers: {max_receivers}, Max balance_changes: {max_balance_changes}, Total activities: {total_scanned}")

        # Second pass: flatten and write to CSV
        def get_all_fieldnames():
            # Build a superset of all possible keys
            fieldnames = set()
            for activity in all_flattened:
                flat = flatten(activity, max_receivers, max_balance_changes)
                fieldnames.update(flat.keys())
            return sorted(fieldnames)

        fieldnames = get_all_fieldnames()
        print(f"Writing CSV with {len(fieldnames)} columns...")

        with open(csv_path, "w", newline="") as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            written = 0
            for activity in all_flattened:
                activity_date = activity.get("date")
                if activity_date and activity_date < after_timestamp:
                    print(f"Stopping CSV write at activity_date={activity_date} (before after_timestamp={after_timestamp})")
                    break
                flat = flatten(activity, max_receivers, max_balance_changes)
                writer.writerow(flat)
                written += 1
            print(f"Done. Total activities written: {written}")

    def list_all_validator_licenses(self) -> list[ValidatorLicense]:
        """
        Lists all validator licenses by paginating through the results.
        Uses a fixed page size of 1000 and continues fetching until next_page_token is null.
        
        Returns:
            List of ValidatorLicense objects, each containing:
                - template_id: str
                - contract_id: str
                - payload: ValidatorLicensePayload containing:
                    - dso: str
                    - sponsor: str
                    - lastActiveAt: str
                    - validator: str
                    - faucetState: ValidatorLicenseFaucetState
                    - metadata: ValidatorLicenseMetadata
                - created_event_blob: str
                - created_at: str
        """
        all_licenses = []
        page_size = 1000
        next_page_token = None
        
        while True:
            response = self.list_validator_licenses(after=next_page_token, limit=page_size)
            licenses = response.get("validator_licenses", [])
            if not licenses:
                break
                
            all_licenses.extend(licenses)
            next_page_token = response.get("next_page_token")
            
            if next_page_token is None:
                break
                
        return all_licenses

    def export_all_round_party_totals_to_csv(
        self,
        csv_path: str,
        start_round: int = 0,
        batch_size: int = 50,
    ) -> None:
        """
        Streams all round party totals from start_round to the latest round to a CSV file.
        Uses list_round_party_totals in batches of 50 rounds.
        For each entry, adds an 'effective_time' column for the round.
        Logs progress to stdout.
        """
        latest_data = self.get_round_of_latest_data()
        latest_round = latest_data.get("round")
        if latest_round is None:
            print("Could not determine latest round.")
            return

        written_header = False
        total_entries = 0
        print(f"Exporting round party totals from round {start_round} to {latest_round} in batches of {batch_size}...")
        with open(csv_path, "w", newline="") as f:
            writer = None
            for batch_start in range(start_round, latest_round + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, latest_round)
                print(f"Fetching rounds {batch_start} to {batch_end}...")
                resp = self.list_round_party_totals(batch_start, batch_end)
                entries = resp.get("entries", [])
                if not entries:
                    print(f"No entries for rounds {batch_start} to {batch_end}.")
                    continue

                # Build a map of closed_round -> effective_time for this batch
                round_to_effective_time = {}
                round_totals = self.list_round_totals(batch_start, batch_end)
                for rt in round_totals:
                    round_num = rt.get("closed_round")
                    eff_time = (
                        rt.get("closed_round_effective_at")
                        or rt.get("effectiveAt")
                        or rt.get("effective_at")
                    )
                    if round_num is not None and eff_time:
                        round_to_effective_time[round_num] = eff_time

                # Add effective_time to each entry
                for entry in entries:
                    round_num = entry.get("closed_round")
                    entry["effective_time"] = round_to_effective_time.get(round_num)

                if not written_header:
                    fieldnames = list(entries[0].keys())
                    if "effective_time" not in fieldnames:
                        fieldnames.append("effective_time")
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    written_header = True
                for entry in entries:
                    writer.writerow(entry)
                total_entries += len(entries)
                print(f"Wrote {len(entries)} entries for rounds {batch_start} to {batch_end} (total so far: {total_entries})")
        print(f"Done writing all round_party_totals to CSV. Total entries: {total_entries}")
        
    def get_wallet_balance_last_10_rounds(
        self,
        party_id: str,
        csv: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get wallet balance for the last 10 rounds for a given party_id.
        
        Args:
            party_id: The party identifier
            csv: If True, writes the results to a CSV file named 'wallet_balance_last_10_rounds_{party_id_short}.csv'
            
        Returns:
            List of dicts, each containing:
                - round: The round number
                - effective_time: The effective time of the round
                - wallet_balance: The wallet balance for that round
        """
        import sys
        
        # Get the latest round
        print(f"[INFO] Getting latest round data...", file=sys.stderr)
        latest_data = self.get_round_of_latest_data()
        latest_round = latest_data.get("round")
        
        if latest_round is None:
            print(f"[ERROR] Could not determine latest round", file=sys.stderr)
            return []
        
        # Calculate the starting round (10 rounds back from latest)
        start_round = max(0, latest_round - 9)  # -9 because we want 10 rounds including the latest
        
        print(f"[INFO] Fetching wallet balances for party_id={party_id} for rounds {start_round} to {latest_round}", file=sys.stderr)
        
        # Get effective times for the rounds
        round_to_effective_time = {}
        entries = self.list_round_totals(start_round, latest_round)
        for entry in entries:
            round_num = entry.get("closed_round")
            eff_time = (
                entry.get("closed_round_effective_at")
                or entry.get("effectiveAt")
                or entry.get("effective_at")
            )
            if round_num is not None and eff_time:
                round_to_effective_time[round_num] = eff_time
        
        # Get wallet balance for each round
        results = []
        for round_num in range(start_round, latest_round + 1):
            balance_resp = self.get_wallet_balance(party_id, round_num)
            wallet_balance = balance_resp.get("wallet_balance")
            effective_time = round_to_effective_time.get(round_num)
            
            print(f"[DEBUG] round={round_num}, effective_time={effective_time}, wallet_balance={wallet_balance}", file=sys.stderr)
            
            results.append({
                "round": round_num,
                "effective_time": effective_time,
                "wallet_balance": wallet_balance,
            })
        
        if csv:
            import csv as _csv
            party_short = party_id.split("::")[0]
            filename = f"wallet_balance_last_10_rounds_{party_short}.csv"
            print(f"[INFO] Writing results to {filename}", file=sys.stderr)
            with open(filename, "w", newline="") as f:
                writer = _csv.DictWriter(f, fieldnames=["round", "effective_time", "wallet_balance"])
                writer.writeheader()
                for row in results:
                    writer.writerow(row)
        
        print(f"[INFO] Done fetching wallet balance for last 10 rounds", file=sys.stderr)
        return results

    def get_holdings_summary_now(
        self,
        migration_id: int,
        owner_party_ids: Optional[List[str]] = None,
    ) -> HoldingsSummaryResponse:
        """
        Get holdings summary at the current time using the latest round.
        Checks the ACS snapshot timestamp to get the most recent available data.
        
        Args:
            migration_id: The migration ID
            owner_party_ids: Optional list of party IDs to filter by
            
        Returns:
            HoldingsSummaryResponse containing:
                - record_time: UTC timestamp of the request
                - migration_id: The migration ID used
                - computed_as_of_round: The round number this was computed for
                - summaries: List of holdings summaries per party, including:
                    - party_id: The party identifier
                    - total_unlocked_coin: Amount of unlocked coins
                    - total_locked_coin: Amount of locked coins
                    - total_coin_holdings: Total coins held
                    - accumulated_holding_fees_unlocked: Accumulated fees for unlocked coins
                    - accumulated_holding_fees_locked: Accumulated fees for locked coins
                    - accumulated_holding_fees_total: Total accumulated fees
                    - total_available_coin: Total available coins
        """

        # Get current UTC time
        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


        # Get the ACS snapshot timestamp for the current time
        acs_snapshot = self.get_acs_snapshot_timestamp(current_time, migration_id)
        snapshot_time = acs_snapshot.get("record_time")

        if not snapshot_time:
            print(f"[WARN] No ACS snapshot found for current time, using current time directly", file=sys.stderr)
            snapshot_time = current_time


        # Get the latest round
        latest_data = self.get_round_of_latest_data()
        latest_round = latest_data.get("round")

        # Build the request data
        data: Dict[str, Any] = {
            "migration_id": migration_id,
            "record_time": snapshot_time,
            "as_of_round": latest_round
        }

        if owner_party_ids is not None:
            data["owner_party_ids"] = owner_party_ids

        return self.session.post(f"{self.base_url}/v0/holdings/summary", json=data).json()