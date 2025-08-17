# 2024年の地方競馬（帯広以外）の全レーススクレイパ
# 改良点:
# - requests.Session + Retry/Timeout
# - 進捗を1レース単位でflush出力
# - バッチ追記保存（途中終了でも成果が残る）
# - チェックポイントを1レース毎に更新
# - SIGTERM/SIGINTで残りを吐いて安全終了

import os
import sys
import time
import json
import re
import random
import signal
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

BASE = "https://keiba.rakuten.co.jp"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KeibaStudyBot/1.0; +https://example.com/)",
}
SLEEP_BETWEEN_REQUESTS = 1.2     # ベーススリープ（秒）
JITTER_MAX = 0.7                 # ランダ