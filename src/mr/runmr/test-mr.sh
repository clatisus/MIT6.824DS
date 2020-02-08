#!/bin/sh

#
# run word-count task against map.go reduce.go
#

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
echo '***' Building
(go build -buildmode=plugin ../../../mrapps/wc.go) || exit 1
(go build ../../../main/mrsequential.go) || exit 1
(go build ../runmr.go) || exit 1

# generate the correct output
echo '***' Generating correct output.
(./mrsequential wc.so ../../../main/pg*txt) || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.
(./runmr wc.so 4 ../../../main/pg*txt) || exit 1
sort mr-out* > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  exit 1
fi
