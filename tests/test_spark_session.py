from unittest.mock import MagicMock, patch

from data_engineering.utils.spark_session import get_spark_session


def test_get_spark_session_reuses_active_session(spark):
    """Em notebook Databricks (cluster clássico OU serverless/Spark Connect),
    já existe uma sessão ativa — reaproveitá-la evita o erro
    CANNOT_CONFIGURE_SPARK_CONNECT_MASTER causado por chamar .master(...)
    quando o Spark Connect já está configurado pelo runtime.
    """
    result = get_spark_session()
    assert result is spark


def test_get_spark_session_builds_local_session_when_none_active():
    """Fora de um notebook (testes, CI, script standalone) não há sessão
    ativa, então uma sessão local mínima deve ser criada.
    """
    fake_session = MagicMock()
    mock_builder = MagicMock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = fake_session

    with (
        patch(
            "data_engineering.utils.spark_session.SparkSession.getActiveSession",
            return_value=None,
        ),
        patch("data_engineering.utils.spark_session.SparkSession.builder", mock_builder),
    ):
        result = get_spark_session(app_name="test-app")

    mock_builder.appName.assert_called_once_with("test-app")
    mock_builder.master.assert_called_once_with("local[*]")
    assert result is fake_session
