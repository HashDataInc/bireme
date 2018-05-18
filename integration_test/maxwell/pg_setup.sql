CREATE TABLE numerictarget (
    aid INTEGER,
    bid INTEGER,
    alpha BIGINT,
    beta REAL,
    gamma NUMERIC(5,2),
    primary key(aid,bid)
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
    beta TIME(5),
    gamma TIMESTAMP(6)
);

CREATE TABLE binarytarget (
    id INTEGER NOT NULL PRIMARY KEY,
    alpha BYTEA,
    beta BOOL,
    gamma BIT(15)
);