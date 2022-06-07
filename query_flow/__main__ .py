import os
from query_flow.parsers import athena_parser, postgres_parser
from query_flow.vizualizers import query_vizualizer


def visualize(queries, metrics, conn_str, engine_name, engine_version="", is_compact=False, execute_query=True, title=""):
    parser = parser_factory(engine_name, engine_version, is_compact, execute_query)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    query_renderer.vizualize(flow_df, title=title, metrics=metrics, open_=True)


def parser_factory(engine_name, engine_version, is_compact, execute_query):
    if engine_name == 'athena':
        return athena_parser.AthenaParser(is_compact=is_compact, execute_query=execute_query)
    elif engine_name in ['postgresql', 'postgres']:
        return postgres_parser.PostgresParser(is_compact=is_compact, execute_query=execute_query)
    else:
        raise NotImplementedError(f"Engine {engine_name}:{engine_version} is not supported")


def run_mocks():
    for plan in [
        'execution_plan_1.json',
        'execution_plan_2.json',
        'execution_plan_3.json',
        'execution_plan_4.json',
        'execution_plan_5.json',
        'execution_plan_6.json',
        'execution_plan_7.json',
        'execution_plan_8.json',
    ]:
        execution_plan = open(f'../tests/parsers/data/athena/parse/{plan}').read()

        # conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"
        p = athena_parser.AthenaParser(execute_query=True)
        flow_df = p.parse([p.execution_plan_extractor(execution_plan)])

        query_renderer = query_vizualizer.QueryVizualizer(p)
        query_renderer.vizualize(flow_df, title=plan, metrics=["nodeOutputRows"], open_=True)
        query_renderer.vizualize(flow_df, title=plan, metrics=['nodeCpuTime'], open_=True)


def run_queries():
    visualize("select 1",
              ['nodeCpuTime'],
              f"awsathena+rest://:@athena.{os.environ['region_name']}.amazonaws.com:443/{os.environ['schema_name']}?s3_staging_dir={os.environ['s3_staging_dir']}",
              "athena")


if __name__ == '__main__':
    # run_mocks()
    run_queries()
