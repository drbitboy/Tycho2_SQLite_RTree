
bin_PROGRAMS = tyc2_test hbc_test

tyc2_test_SOURCES = tyc2_test.c tyc2lib.c tyc2lib.h localmalloc.h
hbc_test_SOURCES = hbc_test.c hbclib.c hbclib.h localmalloc.h

dist_doc_DATA = harvardbincat.py httpgunzip.py tyc2_loadindex_hogan.py hbc_loadindex.py sqlite3ghost.py tyc2_loadindex.py

test: testtyc2 testhbc

testhbc: pyhbctest.out chbctest.out
	md5sum $^ || md5 $^
	diff -y -W110 $^ || true
	diff -s $^ || true
	sum $^

chbctest.out: hbc_test bsc5.sqlite3
	$(RM) $@
	./hbc_test > $@

pyhbctest.out: bsc5.sqlite3
	$(RM) $@
	python hbc_loadindex.py test > $@

bsc5.sqlite3: BSC5
	python hbc_loadindex.py reload | ( python -c 'import sqlite3' 2>/dev/null && cat || sqlite3 $@ )


testtyc2: pytest.out ctest.out
	md5sum $^ || md5 $^
	diff -y -W80 $^ || true
	diff -s $^ || true
	sum $^

ctest.out: tyc2_test tyc2.sqlite3
	$(RM) $@
	./tyc2_test > $@

pytest.out: tyc2.sqlite3
	$(RM) $@
	python tyc2_loadindex.py test > $@

tyc2.sqlite3: index.dat suppl_1.dat catalog.dat
	python tyc2_loadindex.py reload | ( python -c 'import sqlite3' 2>/dev/null && cat || sqlite3 $@ )

clean-local:
	$(RM) ctest.out pytest.out chbctest.out pyhbctest.out

oldclean:
	$(RM) ctest.out pytest.out tyc2_test tyc2_test.o tyc2lib.o \
		chbctest.out pyhbctest.out hbc_test hbc_test.o hbclib.o \
		sqlite3.o

