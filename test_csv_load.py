from instrument_master import InstrumentMaster

CSV_FILE = "api-scrip-master.csv"

master = InstrumentMaster(CSV_FILE)
master.print_nearest_futures()
