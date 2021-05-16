DROP TABLE IF EXISTS Exam ;
DROP TABLE IF EXISTS Student ;
DROP TABLE IF EXISTS School ;
DROP TABLE IF EXISTS Location ;


CREATE TABLE Location (
    locationid  serial primary key,
    regname      VARCHAR(128),
    tername      VARCHAR(128),
    areaname     VARCHAR(128),
    tertypename  VARCHAR(128),
    CONSTRAINT location_unique_constraint UNIQUE (regname, areaname, tername)
);


CREATE TABLE School (
    schoolid             serial primary key,
    eoname                text,
    eotypename            VARCHAR(128),
    eoparent              text,
    locationid           INTEGER,
    CONSTRAINT school_unique_constraint UNIQUE (eoname),
    CONSTRAINT school_location_fk
        FOREIGN KEY(locationid)
        REFERENCES Location (locationid)
);

CREATE TABLE Student (
    OutID            uuid primary key,
    birth                 SMALLINT,
    sextypename           VARCHAR(128),
    locationid  	  INTEGER ,
    schoolid             INTEGER,
   
    CONSTRAINT student_location_fk
        FOREIGN KEY(locationid)
        REFERENCES Location (locationid),
    CONSTRAINT student_school_fk
        FOREIGN KEY(schoolid)
        REFERENCES School (schoolid),
);

CREATE TABLE Exam (
    examid              serial primary key,
    test                VARCHAR(128),
    teststatus          VARCHAR(128),
    year                SMALLINT,
    ball100             SMALLINT,
    ball12              SMALLINT,
    ball                SMALLINT,
    locationid         INTEGER,
    OutID          uuid,
     CONSTRAINT exam_location_fk
        FOREIGN KEY(locationid)
        REFERENCES Location (locationid),
    CONSTRAINT exam_student_fk
        FOREIGN KEY(studentid)
        REFERENCES Student (OutID)
);



