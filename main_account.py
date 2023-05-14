from stats_account import *

import google.auth
# from GoogleApiSupport.auth import get_service
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from apiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools

filename = "Rautureau"
#
# no_col_itemid(filename)
# col_itemid_missrow(filename)

br2(filename)
br_group(filename)
off_on(filename)
top(filename)
calc(filename)
excel(filename)
write(filename)
# # #

# ent = "Egide group"
# get_card(ent)

# top_hotel_paris()