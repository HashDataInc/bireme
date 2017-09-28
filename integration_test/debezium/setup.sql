CREATE TABLE numericsource (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BIGINT,
    beta REAL
);

CREATE TABLE charsource (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha CHAR(5),
    beta VARCHAR(10),
    gamma TEXT
);

CREATE TABLE timesource (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha DATE,
    beta TIME(3)
);

CREATE TABLE binarysource (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BYTEA,
    beta BOOL,
    gamma BIT(15)
);

INSERT INTO numericsource 
VALUES (1, -9223372036854775808, -123.456),
       (2, 9223372036854775807, 12.345),
       (3, 100, null),
       (4, 100, 100),
       (5, 100, 100);

INSERT INTO charsource
VALUES (1, 'rocks', 'hashdata', 'Bireme is an incremental synchronization tool for the Greenplum / HashData data warehouse'),
       (2, 'spare', 'pivotal', 'symble test " \ 
new line');

INSERT INTO timesource
VALUES (1, '2017-09-18', '10:29:00'),
       (2, '2017-09-18', '10:29:00.123');

INSERT INTO binarysource
VALUES (1, decode('1A1B1C3D1F', 'hex'), true, B'110000011101100'),
       (2, decode('ABCDEF1234', 'hex'), false, B'001100111100011');

UPDATE numericsource
SET alpha = 101
WHERE id = 3;

UPDATE numericsource
SET id = 400
WHERE id = 4;

DELETE FROM numericsource
WHERE id = 5;


CREATE TABLE numerictarget (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BIGINT,
    beta REAL
);

CREATE TABLE chartarget (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha CHAR(5),
    beta VARCHAR(10),
    gamma TEXT
);

CREATE TABLE timetarget (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha DATE,
    beta TIME(5)
);

CREATE TABLE binarytarget (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BYTEA,
    beta BOOL,
    gamma BIT(15)
);

