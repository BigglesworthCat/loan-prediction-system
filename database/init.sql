CREATE SCHEMA credits_dataset;

CREATE TABLE credits_dataset.credit_history
(
    age                SMALLINT,
    month_income       INT,
    employment_length  INT,
    home_ownership     VARCHAR(10),
    loan_intent        VARCHAR(20),
    loan_amount        INT,
    loan_interest_rate FLOAT,
    previous_loans     SMALLINT,
    previous_default   VARCHAR(3),
    loan_grade         VARCHAR(1),
    loan_status        SMALLINT NOT NULL
);

COPY credits_dataset.credit_history FROM '/credit_history.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE credits_dataset.credit_history_transformed
(
    age                FLOAT    NOT NULL,
    month_income       FLOAT    NOT NULL,
    employment_length  FLOAT    NOT NULL,
    home_ownership     FLOAT    NOT NULL,
    loan_intent        FLOAT    NOT NULL,
    loan_amount        FLOAT    NOT NULL,
    loan_interest_rate FLOAT    NOT NULL,
    previous_loans     FLOAT    NOT NULL,
    previous_default   FLOAT    NOT NULL,
    loan_grade         FLOAT    NOT NULL,
    loan_status        SMALLINT NOT NULL
);

COPY credits_dataset.credit_history_transformed FROM '/credit_history_transformed.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE credits_dataset.credit_predictions
(
    first_name         VARCHAR(20) NOT NULL,
    last_name          VARCHAR(20) NOT NULL,
    age                SMALLINT    NOT NULL,
    month_income       INT         NOT NULL,
    employment_length  INT         NOT NULL,
    home_ownership     VARCHAR(10) NOT NULL,
    loan_intent        VARCHAR(20) NOT NULL,
    loan_amount        INT         NOT NULL,
    loan_interest_rate FLOAT       NOT NULL,
    previous_loans     SMALLINT    NOT NULL,
    previous_default   VARCHAR(3)  NOT NULL,
    loan_grade         VARCHAR(1)  NOT NULL,
    prediction_result  VARCHAR(10) NOT NULL
);
