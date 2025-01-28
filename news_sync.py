import argparse
import time
import os
from relay_sync import RelaySyncer

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Relay URLs
FEDI_RELAY_URL = "wss://relay.mostr.pub"
NOSTR_RELAY_URL = "wss://relay.nos.social"
OUTPUT_RELAY = "wss://news.nos.social"

NOSTR_PUBLISHERS = [
    "b43cdcbe1b5a991e91636c1372abd046ff1d6b55a17722a2edf2d888aeaa3150", # NPD Media
    "9561cd80e1207f685277c5c9716dde53499dd88c525947b1dde51374a81df0b9", # Revolution Z
    "e526964aad10b63c24b3a582bfab4ef5937c559bfbfff3c18cb8d94909598575", # MuckRock
    "36de364c2ea2a77f2ed42cd7f2528ef547b6ab0062e3645046188511fe106403", # ZNet
    "99d0c998eaf2dbfaead9abf50919eba6495d8d615f0ded6b320948a4a4f8c478", # Patrick Boehler
    "715dc06230d7c6aa62b044a8a764728ae6862eb100f1800ef91d5cc9f972dc55", # We Distribute
    "e70d313e00d3d77c3ca7324c082fce9bbdefbe1b88cf39d4e48078c1573808ed", # The Conversation
    "0403c86a1bb4cfbc34c8a493fbd1f0d158d42dd06d03eaa3720882a066d3a378", # Global Sports Central
    "a78363acf392e7f6805d9d87654082dd83a02c6c565c804533e62b6f1da3f17d", # Alastair Thompson
    "b5ad453f5410107a61fde33b0bf7f61832e96b13f8fd85474355c34818a34091", # The 74
]

FEDI_PUBLISHERS = [
    "2a5ce82d946a0e086f9228f68494f3597e91510c66bd201b442c968cd8381502", # Pro Publica
    "68ac0f27c0545377ec6e7c5ce6aa2d6ef8aa1edadc6a8c2ffae8eda07f26affc", # Robert Reich
    "407069c625e86232ae5c5709a6d2c71ef8df24f61d3c57784ca5404cb10229a0", # Bill Bennet
    "04d1fabc2623f568dc600d7ebb4ea1a13b8ccfdc2c5bca1d955f769f4562e82f", # The Spinoff
    "d4a5cb6ef3627f22a9ac5486716b8d4dc44270898ef16da75d4ba05754cdbdc5", # Dan Slevin
    "fd615dad65d0a6ee443f4e49c0da3e26a264f42ea67d694fdceb38e7abeceb28", # Lucire
    "696736ec91f9b497bf0480f73530abd5c4a3bf8e261cfb23096dd88297a2190f", # Taylor Lorenz
    "82acde23330b88e6831146a373eee2716c57df3e0054c5187169e92ee0880120",  # Al Jazeera
]

LAST_RUN_FILE_NOSTR = os.path.join(SCRIPT_DIR, "news_sync_timestamp_nostr.txt")
LAST_RUN_FILE_FEDI = os.path.join(SCRIPT_DIR, "news_sync_timestamp_fedi.txt")

def parse_arguments():
    parser = argparse.ArgumentParser(description="News synchronization script.")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    return parser.parse_args()

def main():
    start_time = time.time()
    args = parse_arguments()

    nostr_syncer = RelaySyncer(
        input_relay=NOSTR_RELAY_URL,
        output_relay=OUTPUT_RELAY,
        timestamp_file=LAST_RUN_FILE_NOSTR,
        quiet_mode=args.quiet
    )

    fedi_syncer = RelaySyncer(
        input_relay=FEDI_RELAY_URL,
        output_relay=OUTPUT_RELAY,
        timestamp_file=LAST_RUN_FILE_FEDI,
        quiet_mode=args.quiet
    )

    print("Syncing Nostr users")
    nostr_successful = nostr_syncer.fetch_and_publish_events(NOSTR_PUBLISHERS)
    duration = time.time() - start_time
    print(f"- Number of native Nostr users being fetched: {len(NOSTR_PUBLISHERS)}")
    print(f"- Number of notes copied to news.nos.social: {nostr_successful}")
    print(f"- Duration: {duration:.1f} seconds")

    print("\nSyncing Fediverse users")
    fedi_successful = fedi_syncer.fetch_and_publish_events(FEDI_PUBLISHERS)
    duration = time.time() - start_time
    print(f"- Number of Fediverse users being fetched: {len(FEDI_PUBLISHERS)}")
    print(f"- Number of notes copied to news.nos.social: {fedi_successful}")
    print(f"- Duration: {duration:.1f} seconds")

if __name__ == "__main__":
    main()
