INSERT INTO Location (regname, areaname, tername, tertypename)
SELECT DISTINCT Regname, AreaName, TerName, TerTypeName FROM ZNO_RESULTS_19_20
ON CONFLICT DO NOTHING;

INSERT INTO Location (regname, areaname, tername, tertypename)
SELECT DISTINCT EORegName, EOAreaName, EOTerName, null FROM ZNO_RESULTS_19_20
ON CONFLICT DO NOTHING;

INSERT INTO Location (regname, areaname, tername, tertypename)
SELECT DISTINCT UkrPTRegName, UkrPTAreaName, UkrPTTerName, null FROM ZNO_RESULTS_19_20
ON CONFLICT DO NOTHING;

INSERT INTO Location (regname, areaname, tername, tertypename)
SELECT DISTINCT HistPTRegName, HistPTAreaName, HistPTTerName, null FROM ZNO_RESULTS_19_20
ON CONFLICT DO NOTHING;

INSERT INTO Location (regname, areaname, tername, tertypename)
SELECT DISTINCT MathPTRegName, MathPTAreaName, MathPTTerName, null FROM ZNO_RESULTS_19_20
ON CONFLICT DO NOTHING;





INSERT INTO School (eoname, eotypename, eoparent, locationid)
SELECT distinct EOName, EOTypeName, EOParent, LocationID
from ZNO_RESULTS_19_20  zno

left join Location l on 
(zno.EORegName = l.eoregname
	AND zno.EOAreaName = l.eoareaname
        AND zno.EOTerName = l.eotername)
ON CONFLICT DO NOTHING;





INSERT INTO Student(OutID,birth,sextypename,locationid, schoolid) 
SELECT distinct OutID, Birth, SexTypeName, l.LocationID, SchoolID
from ZNO_RESULTS_19_20 zno

left join Location l using (RegName, AreaName, TerName)
	left join School s on (zno.EOName  = s.eoname
                    AND zno.EOTypeName  = s.eotypename
                    AND zno.EOParent = s.eoparent)
ON CONFLICT DO NOTHING;



INSERT INTO Exam(
    test,testname,teststatus, year, ball100, ball12, ,ball,locationid, OutID )

SELECT UkrTest, UkrPTName, UkrTestStatus, Year, UkrBall100, UkrBall12, UkrBall, l.LocationID, OutID
from ZNO_RESULTS_19_20 zno
left join Student st using (OutID)
	left join School s on (zno.EOName  = s.eoname
                    AND zno.EOTypeName  = s.eotypename
                    AND zno.EOParent = s.eoparent)
left join Location l on (zno.UkrPTRegName = l.regname
                    AND zno.UkrPTAreaName = l.areaname
                    AND zno.UkrPTTerName = l.tername)
ON CONFLICT DO NOTHING;



INSERT INTO Exam(
    test,testname,teststatus, year, ball100, ball12, ,ball,locationid, OutID )

SELECT HistTest, HistPTName, HistTestStatus, Year, HistBall100, HistBall12, HistBall, l.LocationID, OutID
from ZNO_RESULTS_19_20 zno
left join Student st using (OutID)
	left join School s on (zno.EOName  = s.eoname
                    AND zno.EOTypeName  = s.eotypename
                    AND zno.EOParent = s.eoparent)
left join Location l on (zno.HistPTRegName = l.regname
                    AND zno.HistPTAreaName = l.areaname
                    AND zno.HistPTTerName = l.tername)
ON CONFLICT DO NOTHING;



INSERT INTO Exam(
    test,testname,teststatus, year, ball100, ball12, ,ball,locationid, OutID )

SELECT MathTest, MathPTName, MathTestStatus, Year, MathBall100, MathBall12, MathBall, l.LocationID, OutID
from ZNO_RESULTS_19_20 zno
left join Student st using (OutID)
	left join School s on (zno.EOName  = s.eoname
                    AND zno.EOTypeName  = s.eotypename
                    AND zno.EOParent = s.eoparent)
left join Location l on (zno.MathPTRegName = l.regname
                    AND zno.MathPTAreaName = l.areaname
                    AND zno.MathPTTerName = l.tername)
ON CONFLICT DO NOTHING;




