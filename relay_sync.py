import json
from datetime import datetime, timezone
from websocket import create_connection, WebSocket
from typing import List, Optional, Dict, Any
import os
import sys
import time
import socket
import uuid  # Add UUID import

class RelaySyncer:
    def __init__(self, input_relay: str, output_relay: str, timestamp_file: Optional[str] = None, quiet_mode: bool = False):
        """
        Initialize the syncer with configuration

        Args:
            input_relay: Input relay URL to fetch from
            output_relay: Output relay URL to publish to
            timestamp_file: File to store last run timestamp (absolute path)
            quiet_mode: Whether to suppress progress output
        """
        self._log("=== Starting RelaySyncer v2 with new error handling ===")
        self.input_relay = input_relay
        self.output_relay = output_relay
        self.timestamp_file = timestamp_file
        self.quiet_mode = quiet_mode
        self.timeout = 30  # Connection timeout in seconds
        self.input_ws: Optional[WebSocket] = None
        self.output_ws: Optional[WebSocket] = None

    def _log(self, message: str) -> None:
        """Internal method for logging messages"""
        # Format specifically for syslog
        sys.stderr.write(f"{message}\n")
        sys.stderr.flush()

    def _debug(self, message: str) -> None:
        """Internal method for debug logging when not in quiet mode"""
        if not self.quiet_mode:
            sys.stderr.write(f"{message}\n")
            sys.stderr.flush()

    def _get_last_run_timestamp(self) -> Optional[int]:
        """
        Internal method to get the timestamp of last successful run

        Returns:
            Optional[int]: Timestamp of last run or None if not available
        """
        if not self.timestamp_file or not os.path.exists(self.timestamp_file):
            return None

        try:
            with open(self.timestamp_file, "r") as file:
                content = file.read().strip()
                return int(float(content))
        except (ValueError, IOError) as e:
            self._log(f"Warning: Error reading {self.timestamp_file}: {e}")
            return None

    def _save_current_timestamp(self) -> None:
        """Internal method to save the current timestamp for future runs"""
        if not self.timestamp_file:
            return

        try:
            os.makedirs(os.path.dirname(self.timestamp_file), exist_ok=True)

            current_time = int(datetime.now(timezone.utc).timestamp())
            with open(self.timestamp_file, "w") as file:
                file.write(str(current_time))
        except IOError as e:
            self._log(f"Error saving timestamp: {e}")

    def _is_connection_closed(self, ws: Optional[WebSocket]) -> bool:
        """Check if a websocket connection is closed or None"""
        return ws is None or not ws.connected

    def _ensure_connection(self, ws: Optional[WebSocket], url: str) -> WebSocket:
        """Ensure we have a valid connection, create new one if needed"""
        if self._is_connection_closed(ws):
            try:
                if ws:
                    ws.close()
            except:
                pass
            try:
                # Set shorter timeouts for initial connection
                return create_connection(url, timeout=10)
            except Exception as e:
                raise ConnectionError(f"Failed to connect to {url}: {e}")
        return ws

    def _with_retry(self, operation_name: str, operation, is_input_relay: bool, max_retries: int = 3, base_timeout: int = 10) -> Any:
        """
        Helper to run an operation with retries and proper connection/error handling

        Args:
            operation_name: Name of operation for logging
            operation: Function to run that takes a WebSocket connection
            is_input_relay: Whether to use input relay (True) or output relay (False)
            max_retries: Maximum number of retry attempts
            base_timeout: Base timeout in seconds for the operation (will increase with retries)

        Returns:
            Result of the operation
        """
        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            # Exponential backoff: timeout doubles with each retry
            current_timeout = base_timeout * (2 ** retry_count)

            try:
                if is_input_relay:
                    self.input_ws = self._ensure_connection(self.input_ws, self.input_relay)
                    self.input_ws.settimeout(current_timeout)
                    ws = self.input_ws
                else:
                    self.output_ws = self._ensure_connection(self.output_ws, self.output_relay)
                    self.output_ws.settimeout(current_timeout)
                    ws = self.output_ws

                if retry_count > 0:
                    self._debug(f"Retry {retry_count + 1}/{max_retries} for {operation_name} with timeout {current_timeout}s")
                return operation(ws)

            except (TimeoutError, socket.timeout) as e:
                last_error = f"Timeout after {current_timeout}s: {e}"
                self._log(f"Error in {operation_name} (attempt {retry_count + 1}/{max_retries}): {last_error}")
            except Exception as e:
                last_error = str(e)
                self._log(f"Error in {operation_name} (attempt {retry_count + 1}/{max_retries}): {last_error}")

            retry_count += 1
            if retry_count == max_retries:
                self._log(f"Operation {operation_name} failed after {max_retries} retries")

            # Cleanup and delay before retry
            try:
                if is_input_relay and self.input_ws:
                    self.input_ws.close()
                    self.input_ws = None
                elif not is_input_relay and self.output_ws:
                    self.output_ws.close()
                    self.output_ws = None
            except:
                pass

            # Exponential backoff for sleep time between retries
            if retry_count < max_retries:
                sleep_time = min(1 * (2 ** retry_count), 30)  # Cap at 30 seconds
                time.sleep(sleep_time)

        return None  # Operation failed after all retries

    def _fetch_operation(self, ws: WebSocket, pubkey: str, since: Optional[int]) -> List[Dict[str, Any]]:
        """
        Internal method to perform the actual fetch operation on a websocket

        Args:
            ws: WebSocket connection to use
            pubkey: Public key to fetch events for
            since: Optional timestamp to fetch events since

        Returns:
            List of events fetched
        """
        events = []
        request = {"authors": [pubkey]}
        if since is not None:
            request["since"] = since

        ws.send(json.dumps(["REQ", str(uuid.uuid4()), request]))  # Generate unique subscription ID

        while True:
            response = ws.recv()
            data = json.loads(response)
            if data[0] == "EOSE":
                break
            if data[0] == "EVENT":
                events.append(data[2])

        if events:
            self._debug(f"Fetched {len(events)} events for {pubkey}")
        return events

    def _publish_operation(self, ws: WebSocket, event: Dict[str, Any]) -> bool:
        """
        Internal method to perform the actual publish operation on a websocket

        Args:
            ws: WebSocket connection to use
            event: Event to publish

        Returns:
            True if publish was successful
        """
        request = json.dumps(["EVENT", {
            "pubkey": event["pubkey"],
            "kind": event["kind"],
            "content": event["content"],
            "created_at": event["created_at"],
            "tags": event["tags"],
            "sig": event["sig"],
            "id": event["id"]
        }])

        ws.send(request)
        response = ws.recv()
        response_data = json.loads(response)

        if response_data[2] != True:
            raise Exception(f"Failed to publish. Response: {response_data}")
        return True

    def _fetch_events_for_pubkey(self, pubkey: str, since: Optional[int]) -> List[Dict[str, Any]]:
        """
        Internal method to fetch events for a single pubkey

        Args:
            pubkey: Public key to fetch events for
            since: Optional timestamp to fetch events since

        Returns:
            List of events
        """
        return self._with_retry(
            operation_name=f"fetch events for {pubkey}",
            operation=lambda ws: self._fetch_operation(ws, pubkey, since),
            is_input_relay=True,
            base_timeout=15
        ) or []

    def _publish_events(self, events: List[Dict[str, Any]]) -> int:
        """
        Internal method to publish events to the output relay

        Args:
            events: List of events to publish

        Returns:
            Number of successfully published events
        """
        successful = 0

        for event in events:
            if self._with_retry(
                operation_name=f"publish event {event['id']}",
                operation=lambda ws: self._publish_operation(ws, event),
                is_input_relay=False,
                base_timeout=10
            ):
                successful += 1

        return successful

    def fetch_and_publish_events(self, pubkeys: List[str]) -> int:
        """
        Fetch events from input relay and publish to output relay.
        Uses the last run timestamp automatically if a timestamp file is configured.

        Args:
            pubkeys: List of pubkeys to fetch events for

        Returns:
            Number of successfully published events
        """
        successful_syncs = 0
        since = self._get_last_run_timestamp()

        if since:
            dt = datetime.fromtimestamp(since, timezone.utc)
            self._debug(f"Fetching events since {dt}")
        self._debug(f"Fetching from {self.input_relay}...")

        for pubkey in pubkeys:
            events = self._fetch_events_for_pubkey(pubkey, since)
            if events:
                self._debug(f"Publishing {len(events)} events for {pubkey} to {self.output_relay}...")
                successful_syncs += self._publish_events(events)

            self._save_current_timestamp()

        return successful_syncs