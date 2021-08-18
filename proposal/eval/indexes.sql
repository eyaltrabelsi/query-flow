CREATE INDEX lineitem_l_orderkey_idx ON "lineitem" ("l_orderkey");
CREATE INDEX lineitem_l_partkey_idx ON "lineitem" ("l_partkey");
CREATE INDEX partsupp_ps_partkey_idx ON "partsupp" ("ps_partkey");
CREATE INDEX partsupp_ps_suppkey_idx ON "partsupp" ("ps_suppkey");
