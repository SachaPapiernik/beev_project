import unittest
from unittest.mock import patch, Mock, call
import pandas as pd
from function import build_engine, load_env, plot_QpYpET, run_query, load_data, create_table

class TestBuildEngine(unittest.TestCase):

    @patch('function.load_env')
    @patch('function.create_engine')
    def test_build_engine(self, mock_create_engine, mock_load_env):
        mock_load_env.return_value = ('localhost', '5432', 'test_db', 'test_user', 'test_password')

        # Call the build_engine function
        engine = build_engine()

        # Assertions
        mock_load_env.assert_called_once()
        mock_create_engine.assert_called_once_with('postgresql://test_user:test_password@localhost:5432/test_db')
        self.assertEqual(engine, mock_create_engine.return_value)

class TestLoadEnv(unittest.TestCase):

    @patch('function.load_dotenv')
    @patch('function.os.getenv')
    def test_load_env(self, mock_getenv, mock_load_dotenv):
        mock_getenv.return_value = 'localhost'

        result = load_env()

        # Assertions
        mock_load_dotenv.assert_called_once()
        mock_getenv.assert_any_call("DB_HOST")
        self.assertEqual(result, ('localhost', 'localhost', 'localhost', 'localhost', 'localhost'))

    def test_missing_environment_variable(self):
        with self.assertRaises(EnvironmentError):
            with patch('function.os.getenv', side_effect=['localhost', '5432', 'test_db', 'test_user', None]):
                load_env()

class TestPlotQpYpET(unittest.TestCase):

    @patch('function.run_query')
    @patch('function.sns.barplot')
    @patch('function.plt.show')
    def test_plot_QpYpET(self, mock_show, mock_barplot, mock_run_query):
        mock_run_query.return_value = pd.DataFrame({
            'Year': [2021, 2021, 2022, 2022],
            'Engine_Type': ['Gas', 'Electric', 'Gas', 'Electric'],
            'total': [100, 50, 120, 60]
        })

        engine = Mock()
        plot_QpYpET(engine)

        # Assertions
        mock_run_query.assert_called_once_with(engine,  """
    SELECT consumer_data."Year", car_data."Engine_Type", SUM(consumer_data."Sales_Volume") as total
    FROM car_data
    INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                            AND car_data."Model" = consumer_data."Model"
    GROUP BY consumer_data."Year", car_data."Engine_Type"
    ORDER BY consumer_data."Year";
    """)

        mock_barplot.assert_called_once()
        mock_show.assert_called_once()

class TestRunQuery(unittest.TestCase):

    @patch('function.pd.read_sql_query')
    def test_run_query(self, mock_read_sql_query):
       
        expected_result = pd.DataFrame({
            'Year': [2021, 2021, 2022, 2022],
            'Engine_Type': ['Gas', 'Electric', 'Gas', 'Electric'],
            'total': [100, 50, 120, 60]
        })
        mock_read_sql_query.return_value = expected_result

        engine = Mock()
        sql_query = "SELECT * FROM some_table"
        result_df = run_query(engine, sql_query)

        # Assertions
        mock_read_sql_query.assert_called_once_with(sql_query, engine)
        pd.testing.assert_frame_equal(result_df, expected_result)

class TestLoadData(unittest.TestCase):

    @patch('function.pd.read_csv')
    @patch('function.pd.DataFrame.to_sql')
    def test_load_data(self, mock_to_sql, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame({
            'Make': ['Toyota', 'Honda'],
            'Model': ['Camry', 'Civic'],
            'Year': [2021, 2022],
            'Sales_Volume': [100, 150]
        })

        engine = Mock()
        csv_filename = 'sample_data.csv'
        tablename = 'sample_table'
        columns = ['Make', 'Model', 'Year', 'Sales_Volume']
        load_data(csv_filename, engine, tablename, columns)

        # Assertions
        mock_read_csv.assert_called_once_with(csv_filename, names=columns, skiprows=1)
        mock_to_sql.assert_called_once_with(tablename, engine, if_exists='append', index=False)

class TestCreateTable(unittest.TestCase):

    @patch('function.Table.create')
    def test_create_table(self, mock_table_create):

        engine = Mock()
        create_table(engine)

        # Assertions
        expected_calls = [
            call(engine),
            call(engine),
        ]
        mock_table_create.assert_has_calls(expected_calls)

if __name__ == '__main__':
    unittest.main()