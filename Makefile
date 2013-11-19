
LDLIBS=-lsqlite3

test: pytest.out ctest.out
	md5sum $^
	diff -y -W80 $^
	diff -s $^

tyc2_test:: tyc2_test.o tyc2lib.o

ctest.out: tyc2_test tyc2.sqlite3
	$(RM) $@
	./tyc2_test > $@

pytest.out: tyc2.sqlite3
	$(RM) $@
	python tyc2_loadindex.py test > $@

tyc2.sqlite3:
	python tyc2_loadindex.py reload

clean:
	$(RM) ctest.out pytest.out tyc2_test tyc2_test.o tyc2lib.o sqlite3.o

