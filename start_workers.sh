#!/bin/bash

python worker.py --user fake1 --password qwerqwer &
python worker.py --user fake2 --password asdfasdf &
python worker.py --user fake3 --password zxcvzxcv &
