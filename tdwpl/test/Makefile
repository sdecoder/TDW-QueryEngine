##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#						    <canma@tencent.com>
#
# Time-stamp: <2010-03-20 10:06:01 macan>
#
# This is the makefile for TDW/PL project.
#
# Armed by EMACS.

TDWPL = $(HOME_PATH)/tdwpl
TDWPL_UT_FLAG = -atestshell

TDWPL_TEST_CASE = tdwpl_case1.ut tdwpl_case2.ut tdwpl_cmd.ut tdwpl_case3.ut

all : $(TDWPL) $(TDWPL_TEST_CASE:.ut=.res)
	@echo -e "  Done."

view : $(TDWPL_TEST_CASE:.ut=.res)
	@echo -e " " VIEW"\t" $^
	@emacs $^

viewcheck : $(TDWPL_TEST_CASE:.ut=.check)
	@echo -e " " VIEW"\t" $^
	@emacs $^

check : $(TDWPL_TEST_CASE:.ut=.check)
	@echo -e " " CHECK Done

%.check : %.res
	@echo -e " " CHECK"\t" $<
	@sed -e '/^Checking/d;/.*server.*/d;/^Module/d;/.*Session/d;/.*File/d;' $< > $<.1
	@sed -e '/^Checking/d;/.*server.*/d;/^Module/d;/.*Session/d;/.*File/d;' $(<:.res=.std) > $<.2
	@diff -Nur -s $<.1 $<.2 > $@
	@rm -rf $<.1 $<.2

%.res : %.ut
	@echo -e " " TEST"\t" $<
	@cat $< | $(TDWPL) $(TDWPL_UT_FLAG) > $@

mcheck: $(TDWPL_TEST_CASE:.ut=.mcheck)
	@echo -e " " CHECK Done

%.mcheck : %.ut
	@echo -e " " CHECK"\t" $<
	@./multiclient.sh $(<:.ut=) $@ check

mc : $(TDWPL_TEST_CASE:.ut=.mres)

%.mres : %.ut
	@echo -e " " TEST"\t" $<
	@./multiclient.sh $< $@ run

clean :
	-@rm -rf $(TDWPL_TEST_CASE:.ut=.res)
	-@rm -rf $(TDWPL_TEST_CASE:.ut=.check)

cleancheck :
	-@rm -rf $(TDWPL_TEST_CASE:.ut=.check)

mclean :
	-@rm -rf *.mres
