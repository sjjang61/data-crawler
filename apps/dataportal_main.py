import argparse
from apps.dataportal.dataportal_apart_rent import DataPortalAptRent
from apps.dataportal.dataportal_apart_trade import DataPortalAptTrade
from apps.dataportal.dataportal_land_trade import DataPortalLandTrade
from apps.dataportal.dataportal_store_trade import DataPortalStoreTrade


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-cmd", "--command", help="command(apt_trade,apt_rent,..)", required=True)
    parser.add_argument("-st", "--start_month", help="start_month", required=True)
    parser.add_argument("-end", "--end_month", help="end_month", required=True)
    parser.add_argument("-loc", "--loc_cd", required=False)
    parser.add_argument("-sido", "--sido_cd", required=False)

    args = parser.parse_args()
    print("[Configure]")
    print("\t- Command : %s" % (args.command))
    print("\t- Month : %s ~ %s" % (args.start_month, args.end_month))
    print("\t- Location Cd : %s" % (args.loc_cd))
    print("\t- Sido Cd : %s" % (args.sido_cd))
    return args


if __name__ == "__main__":

    args = arg_parse()
    if args.command == 'apt_trade':
        dataportal = DataPortalAptTrade()
    elif args.command == 'apt_rent':
        dataportal = DataPortalAptRent()
    elif args.command == 'land_trade':
        dataportal = DataPortalLandTrade()
    elif args.command == 'store_trade':
        dataportal = DataPortalStoreTrade()
    else:
        print(f"Unknown command = {args.command}")
        exit(0)

    dataportal.process( args.start_month, args.end_month, args.loc_cd, args.sido_cd )
