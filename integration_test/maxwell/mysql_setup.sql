CREATE TABLE numericsource (
    aid INTEGER,
    bid INTEGER,
    alpha BIGINT,
    beta REAL,
    gamma NUMERIC(5,2),
    PRIMARY KEY(aid,bid)
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
    beta TIME(3),
    gamma TIMESTAMP(6)
);

CREATE TABLE binarysource (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BINARY(5),
    beta BOOL,
    gamma BIT(15)
);

INSERT INTO numericsource 
VALUES (1, 1, -9223372036854775808, -123.456, 10.1),
       (1, 2, 9223372036854775807, 12.345, 10),
       (3, 1, 100, null, 10.0),
       (3, 2, 100, 100, 10.01),
       (1, 3, 100, 100, null);

INSERT INTO charsource
VALUES (1, 'rocks', 'hashdata', 'Bireme is an incremental synchronization tool for the Greenplum / HashData data warehouse'),
       (2, 'spare', 'pivotal', 'symble test " \ 
new line'),
       (3, 'abcde', '', '');

INSERT INTO timesource
VALUES (1, '2017-09-18', '10:29:00', '2017-09-29 15:00:00.0'),
       (2, '2017-09-18', '10:29:00.123', '2017-09-29 15:00:00.0123');

INSERT INTO binarysource
VALUES (1, X'1A1B1C3D1F', false, B'110000011101100'),
       (2, X'ABCDEF1234', true, B'001100111100011'),
       (3, X'1A2B3C4D5E', false, B'101010011001100');
