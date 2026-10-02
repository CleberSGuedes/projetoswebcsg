from config import Config


def test_config_uses_mysql():
    assert Config.SQLALCHEMY_DATABASE_URI.startswith("mysql+pymysql://")


def test_config_max_content_length_configured():
    assert Config.MAX_CONTENT_LENGTH is not None
    assert Config.MAX_CONTENT_LENGTH > 0
