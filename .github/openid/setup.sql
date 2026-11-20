-- Users authenticated by the JWT integration are external: they have no
-- StarRocks account, so they get privileges through a group mapping.
CREATE GROUP PROVIDER dbt_groups PROPERTIES (
    "type" = "file",
    "group_file_url" = "dbt_groups.txt"
);

CREATE SECURITY INTEGRATION keycloak PROPERTIES (
    "type" = "authentication_jwt",
    "jwks_url" = "http://keycloak:8080/realms/dbt/protocol/openid-connect/certs",
    "principal_field" = "preferred_username",
    "required_issuer" = "http://keycloak:8080/realms/dbt",
    "required_audience" = "dbt-starrocks",
    "group_provider" = "dbt_groups",
    "permitted_groups" = "dbt"
);

ADMIN SET FRONTEND CONFIG ("authentication_chain" = "native,keycloak");

GRANT db_admin TO EXTERNAL GROUP dbt;
