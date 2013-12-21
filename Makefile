
CFLAGS=$(shell curl-config --cflags 2>/dev/null || echo -DSKIP_SIMBAD) $(CDEBUG)

LDLIBS=-lsqlite3 $(shell curl-config --libs 2>/dev/null) $(CDEBUG)

test: testtyc2 testhbc

testhbc: pyhbctest.out chbctest.out
	md5sum $^
	diff -y -W110 $^ || true
	diff -s $^ || true
	sum $^

tyc2_test:: tyc2_test.o tyc2lib.o
hbc_test:: hbc_test.o hbclib.o

chbctest.out: hbc_test bsc5.sqlite3
	$(RM) $@
	./hbc_test > $@

pyhbctest.out: bsc5.sqlite3
	$(RM) $@
	python hbc_loadindex.py test > $@

bsc5.sqlite3:
	python hbc_loadindex.py reload


testtyc2: pytest.out ctest.out
	md5sum $^
	diff -y -W80 $^ || true
	diff -s $^ || true
	sum $^

ctest.out: tyc2_test tyc2.sqlite3
	$(RM) $@
	./tyc2_test > $@

pytest.out: tyc2.sqlite3
	$(RM) $@
	python tyc2_loadindex.py test > $@

tyc2.sqlite3:
	python tyc2_loadindex.py reload

clean:
	$(RM) ctest.out pytest.out tyc2_test tyc2_test.o tyc2lib.o \
		chbctest.out pyhbctest.out hbc_test hbc_test.o hbclib.o \
		sqlite3.o

