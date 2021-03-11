CREATE TABLE IF NOT EXISTS u_user (
    id          bigint         NOT NULL,
    create_time datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name        varchar(30)    NOT NULL DEFAULT '',
    nick_name   varchar(30)    NOT NULL DEFAULT '',
    balance     decimal(14, 2) NOT NULL DEFAULT 0.00,
    height      int            NULL,
    love_music  longblob       NULL,
    PRIMARY KEY (id)
)
