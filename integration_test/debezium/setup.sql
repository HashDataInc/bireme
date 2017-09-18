CREATE TABLE numerictype (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BIGINT,
    beta NUMERIC(5, 2),
    gamma REAL
);

CREATE TABLE chartype (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha CHAR(5),
    beta VARCHAR(10),
    gamma TEXT
);

CREATE TABLE timetype (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha TIME(5),
    beta DATE,
    gamma TIME(5)
);

CREATE TABLE binarytype (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BYTEA,
    beta BOOL,
    gamma BIT(15)
);

INSERT INTO numerictype 
VALUES (1, -9223372036854775808, -999.99, -123.456),
       (2, 9223372036854775807, 666.66, 12.345),
       (3, 100, 100, 100),
       (4, 100, 100, 100),
       (5, 100, 100, 100);

INSERT INTO chartype
VALUES (1, 'rocks', 'hashdata', 'Bireme is an incremental synchronization tool for the Greenplum / HashData data warehouse'),
       (2, 'spare', 'pivotal', 'symble test " \ 
new line');

INSERT INTO timetype
VALUES (1, '2017-09-18 10:29:00', '2017-09-18', '10:29:00'),
       (2, '2017-09-18 10:29:00.321', '2017-09-18', '10:29:00.12345');

INSERt INTO binarytype
VALUES (1, decode('1A1B1C3D1F', 'hex'), true, B'110000011101100'),
       (2, decode('ABCDEF1234', 'hex'), false, B'001100111100011');

UPDATE numerictype
SET alpha = 100
WHERE id = 3;

UPDATE numerictype
SET id = 400
WHERE id = 4;

DELETE FROM numerictype
WHERE id = 5;

