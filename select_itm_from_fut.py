# select_itm_from_fut.py
from instrument_master import InstrumentMaster

CSV_FILE = "api-scrip-master.csv"   # keep same file name

def pick_itm(index_name, fut_ltp):
    master = InstrumentMaster(CSV_FILE)

    # step sizes
    step_map = {
        "NIFTY": 50,
        "FINNIFTY": 50,
        "BANKNIFTY": 100
    }

    step = step_map[index_name]

    itm = master.get_itm_ce_pe(
        index_name=index_name,
        fut_ltp=fut_ltp,
        strike_step=step,
        itm_steps=1
    )

    print("\n==============================")
    print(f"INDEX      : {index_name}")
    print(f"FUT LTP    : {fut_ltp}")
    print(f"ATM        : {itm['atm']}")
    print("------ ITM CE ------")
    print(itm["ce"])
    print("------ ITM PE ------")
    print(itm["pe"])
    print("==============================\n")

if __name__ == "__main__":
    # manually paste current FUT LTP (from your live feed)
    index = input("Enter INDEX (NIFTY / BANKNIFTY / FINNIFTY): ").strip().upper()
    ltp = float(input("Enter FUT LTP: "))

    pick_itm(index, ltp)
