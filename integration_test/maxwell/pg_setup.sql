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