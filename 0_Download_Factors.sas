options error=1;

libname factor "D:\Leadlag\replication\formal_tests\jingda_2511\tmp";
/*log into wrds via PC SAS connect*/
%let wrds = wrds.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=jingda12 password="~!@Aayanjingda1234";
rsubmit;

libname ff "/wrds/ff/sasdata";
libname crsp ('/wrds/crsp/sasdata/a_stock','/wrds/crsp/sasdata/m_stock',
	'/wrds/crsp/sasdata/a_ccm');

    proc sql;
        create table work.fama_french as
        select date, mktrf, smb, hml, rf, umd
        from ff.factors_monthly
        where date >= '01JAN1926'd;
    quit;

    proc sql;
        create table work.liquidity as
        select date, ps_vwf
        from ff.liq_ps
        where date >= '01JAN1960'd;
    quit;



proc download data=fama_french out=factor.fama_french;
run;
proc download data=liquidity out=factor.liquidity;
run;
endrsubmit;
* 5. Sign off from the WRDS server.;
signoff;

data factor.liquidity;
set factor.liquidity;
if ps_vwf >= -1;
run;


proc sql;
create table factor.factors_all
as select a.*, b.ps_vwf as liquidity
from factor.fama_french as a left join  factor.liquidity as b
on intnx('month', a.date, 0, 'end') = intnx('month', b.date, 0, 'end');
quit;
