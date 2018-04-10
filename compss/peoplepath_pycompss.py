#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Auto-generated COMPSs code from Lemonade Workflow
# (c) Speed Labs - Departamento de Ciência da Computação
#     Universidade Federal de Minas Gerais
# More information about Lemonade to be provided
#

from timeit import default_timer as timer
from functions.data.ReadData import ReadCSVFromHDFSOperation
from functions.data.AttributesChanger import AttributesChangerOperation
from functions.geo.ReadShapeFile import ReadShapeFileOperation
from functions.etl.Filter import FilterOperation
from functions.etl.Transform import TransformOperation
from functions.etl.Select import SelectOperation
from functions.etl.Sort import SortOperation
from functions.etl.Aggregation import AggregationOperation
from functions.etl.Join import JoinOperation
from functions.etl.Distinct import DistinctOperation
from functions.geo.GeoWithin import GeoWithinOperation
from functions.data.SaveData import SaveHDFSOperation


def compss_logging(msg):
    print msg


def take_sample(data):
    pass


def emit_event(name=None, message=None, status=None, identifier=None):
    pass

task_futures = {}


def data_reader_0(compss_session, cached_state, emit_event):
    """
    read Tickeing data
    """

    compss_logging(
        "Submitting parent task [] before 45163792-5078-4af9-ab2d-d3357f1cdd8e")

    # Next we wait for the dependencies to complete

    compss_logging(
        "Parents completed, submitting 45163792-5078-4af9-ab2d-d3357f1cdd8e")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='45163792-5078-4af9-ab2d-d3357f1cdd8e')

    compss_logging(
        "Lemonade task 45163792-5078-4af9-ab2d-d3357f1cdd8e started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['port'] = 9000
    settings['host'] = 'localhost'
    settings['path'] = '/ticket.csv'
    settings['header'] = True
    settings['separator'] = ';'
    settings['infer'] = 'FROM_VALUES'
    settings['mode'] = 'DROPMALFORMED'

    data20 = ReadCSVFromHDFSOperation(settings, numFrag)

    results = {
        'output data': {'output': data20, 'sample': take_sample(data20)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='45163792-5078-4af9-ab2d-d3357f1cdd8e')

    compss_logging(
        "Lemonade task 45163792-5078-4af9-ab2d-d3357f1cdd8e completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def data_reader_1(compss_session, cached_state, emit_event):
    """
    Census Data
    """

    compss_logging(
        "Submitting parent task [] before 7458cad9-0a82-487a-b65a-f55a17039757")

    # Next we wait for the dependencies to complete

    compss_logging(
        "Parents completed, submitting 7458cad9-0a82-487a-b65a-f55a17039757")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='7458cad9-0a82-487a-b65a-f55a17039757')

    compss_logging(
        "Lemonade task 7458cad9-0a82-487a-b65a-f55a17039757 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['port'] = 9000
    settings['host'] = 'localhost'
    settings['path'] = '/census.csv'
    settings['header'] = True
    settings['separator'] = ';'
    settings['infer'] = 'FROM_VALUES'
    settings['mode'] = 'DROPMALFORMED'

    data18 = ReadCSVFromHDFSOperation(settings, numFrag)

    results = {
        'output data': {'output': data18, 'sample': take_sample(data18)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='7458cad9-0a82-487a-b65a-f55a17039757')

    compss_logging(
        "Lemonade task 7458cad9-0a82-487a-b65a-f55a17039757 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def change_attribute_2(compss_session, cached_state, emit_event):
    """ Operation 3d9260ef-f037-45fa-b339-da280ed00c0a """

    compss_logging(
        "Submitting parent task [u'45163792-5078-4af9-ab2d-d3357f1cdd8e'] before 3d9260ef-f037-45fa-b339-da280ed00c0a")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['45163792-5078-4af9-ab2d-d3357f1cdd8e']
    data20, pr_data20 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data20 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 3d9260ef-f037-45fa-b339-da280ed00c0a")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='3d9260ef-f037-45fa-b339-da280ed00c0a')

    compss_logging(
        "Lemonade task 3d9260ef-f037-45fa-b339-da280ed00c0a started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['new_data_type'] = 'Date/time'
    settings['attributes'] = [u'DATAUTILIZACAO']
    settings['new_name'] = [u'DATAUTILIZACAO']
    data12 = AttributesChangerOperation(data20, settings, numFrag)

    results = {
        'output data': {'output': data12, 'sample': take_sample(data12)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='3d9260ef-f037-45fa-b339-da280ed00c0a')

    compss_logging(
        "Lemonade task 3d9260ef-f037-45fa-b339-da280ed00c0a completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def read_shapefile_3(compss_session, cached_state, emit_event):
    """ Operation 4360919c-d6b4-4d3b-b9b2-f44037c19c15 """

    compss_logging(
        "Submitting parent task [] before 4360919c-d6b4-4d3b-b9b2-f44037c19c15")

    # Next we wait for the dependencies to complete

    compss_logging(
        "Parents completed, submitting 4360919c-d6b4-4d3b-b9b2-f44037c19c15")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='4360919c-d6b4-4d3b-b9b2-f44037c19c15')

    compss_logging(
        "Lemonade task 4360919c-d6b4-4d3b-b9b2-f44037c19c15 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['polygon'] = 'points'
    settings['shp_path'] = '/41CURITI.shp'
    settings['dbf_path'] = '/41CURITI.dbf'
    ReadShapeFileOperation(settings)

    results = {
        'geo data': {'output': data6, 'sample': take_sample(data6)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='4360919c-d6b4-4d3b-b9b2-f44037c19c15')

    compss_logging(
        "Lemonade task 4360919c-d6b4-4d3b-b9b2-f44037c19c15 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def filter_selection_4(compss_session, cached_state, emit_event):
    """ Operation 8a564cb8-65b4-4d26-a71a-378e3fff0379 """

    compss_logging(
        "Submitting parent task [u'4360919c-d6b4-4d3b-b9b2-f44037c19c15'] before 8a564cb8-65b4-4d26-a71a-378e3fff0379")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['4360919c-d6b4-4d3b-b9b2-f44037c19c15']
    data6, pr_data6 = (parent_result['geo data'][
                       'output'], parent_result['geo data']['sample'])
    ts_out_task_1 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 8a564cb8-65b4-4d26-a71a-378e3fff0379")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='8a564cb8-65b4-4d26-a71a-378e3fff0379')

    compss_logging(
        "Lemonade task 8a564cb8-65b4-4d26-a71a-378e3fff0379 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['query'] = "(NOMEMICR == CURITIBA) "
    data23 = FilterOperation(data6, settings, numFrag)

    results = {
        'output data': {'output': data23, 'sample': take_sample(data23)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='8a564cb8-65b4-4d26-a71a-378e3fff0379')

    compss_logging(
        "Lemonade task 8a564cb8-65b4-4d26-a71a-378e3fff0379 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def transformation_5(compss_session, cached_state, emit_event):
    """ Operation a8d54724-6087-4a18-8d3b-f53ebecb4328 """

    compss_logging(
        "Submitting parent task [u'3d9260ef-f037-45fa-b339-da280ed00c0a'] before a8d54724-6087-4a18-8d3b-f53ebecb4328")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['3d9260ef-f037-45fa-b339-da280ed00c0a']
    data12, pr_data12 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data12 = parent_result['time']

    compss_logging(
        "Parents completed, submitting a8d54724-6087-4a18-8d3b-f53ebecb4328")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='a8d54724-6087-4a18-8d3b-f53ebecb4328')

    compss_logging(
        "Lemonade task a8d54724-6087-4a18-8d3b-f53ebecb4328 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['functions'] = [[u'BINS_5_MINS',
                              "lambda col: group_datetime(col['DATAUTILIZACAO'], 300)", '']]
    data3 = TransformOperation(data12, settings, numFrag)

    results = {
        'output data': {'output': data3, 'sample': take_sample(data3)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='a8d54724-6087-4a18-8d3b-f53ebecb4328')

    compss_logging(
        "Lemonade task a8d54724-6087-4a18-8d3b-f53ebecb4328 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def projection_6(compss_session, cached_state, emit_event):
    """
    Create date time bins of 5 minutes
    """

    compss_logging(
        "Submitting parent task [u'a8d54724-6087-4a18-8d3b-f53ebecb4328'] before e512da41-bd81-4213-a4f7-19113652a3c3")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['a8d54724-6087-4a18-8d3b-f53ebecb4328']
    data3, pr_data3 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data3 = parent_result['time']

    compss_logging(
        "Parents completed, submitting e512da41-bd81-4213-a4f7-19113652a3c3")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='e512da41-bd81-4213-a4f7-19113652a3c3')

    compss_logging(
        "Lemonade task e512da41-bd81-4213-a4f7-19113652a3c3 started")

    start = timer()
    numFrag = 16
    columns = [
        "BINS_5_MINS",
        "CODLINHA",
        "CODVEICULO",
        "NUMEROCARTAO",
        "DATAUTILIZACAO"]
    data5 = SelectOperation(data3, columns, numFrag)

    results = {
        'output projected data': {'output': data5, 'sample': take_sample(data5)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='e512da41-bd81-4213-a4f7-19113652a3c3')

    compss_logging(
        "Lemonade task e512da41-bd81-4213-a4f7-19113652a3c3 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def sort_7(compss_session, cached_state, emit_event):
    """
    Sort the records per line, vehicle, bin and datetime
    """

    compss_logging(
        "Submitting parent task [u'e512da41-bd81-4213-a4f7-19113652a3c3'] before ef8c53c4-0c3b-428d-bafa-18d3e82704d5")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['e512da41-bd81-4213-a4f7-19113652a3c3']
    data5, pr_data5 = (parent_result['output projected data'][
                       'output'], parent_result['output projected data']['sample'])
    ts_data5 = parent_result['time']

    compss_logging(
        "Parents completed, submitting ef8c53c4-0c3b-428d-bafa-18d3e82704d5")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='ef8c53c4-0c3b-428d-bafa-18d3e82704d5')

    compss_logging(
        "Lemonade task ef8c53c4-0c3b-428d-bafa-18d3e82704d5 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['columns'] = [
        u'CODLINHA',
        u'CODVEICULO',
        u'BINS_5_MINS',
        u'DATAUTILIZACAO',
        u'NUMEROCARTAO']
    settings['ascending'] = [True, True, True, True, True]
    settings['algorithm'] = 'bitonic'
    data2 = SortOperation(data5, settings, numFrag)

    results = {
        'output data': {'output': data2, 'sample': take_sample(data2)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='ef8c53c4-0c3b-428d-bafa-18d3e82704d5')

    compss_logging(
        "Lemonade task ef8c53c4-0c3b-428d-bafa-18d3e82704d5 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def data_reader_8(compss_session, cached_state, emit_event):
    """
    Read GPS data
    """

    compss_logging(
        "Submitting parent task [] before 86794e6d-5e98-4608-9633-ceaa4a79e3a3")

    # Next we wait for the dependencies to complete

    compss_logging(
        "Parents completed, submitting 86794e6d-5e98-4608-9633-ceaa4a79e3a3")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='86794e6d-5e98-4608-9633-ceaa4a79e3a3')

    compss_logging(
        "Lemonade task 86794e6d-5e98-4608-9633-ceaa4a79e3a3 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['port'] = 9000
    settings['host'] = 'localhost'
    settings['path'] = '/gps.csv'
    settings['header'] = True
    settings['separator'] = ';'
    settings['infer'] = 'FROM_VALUES'
    settings['mode'] = 'DROPMALFORMED'

    data0 = ReadCSVFromHDFSOperation(settings, numFrag)

    results = {
        'output data': {'output': data0, 'sample': take_sample(data0)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='86794e6d-5e98-4608-9633-ceaa4a79e3a3')

    compss_logging(
        "Lemonade task 86794e6d-5e98-4608-9633-ceaa4a79e3a3 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def change_attribute_9(compss_session, cached_state, emit_event):
    """ Operation 8d32f0dc-1f15-4276-b887-a66c28a2b5ba """

    compss_logging(
        "Submitting parent task [u'86794e6d-5e98-4608-9633-ceaa4a79e3a3'] before 8d32f0dc-1f15-4276-b887-a66c28a2b5ba")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['86794e6d-5e98-4608-9633-ceaa4a79e3a3']
    data0, pr_data0 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data0 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 8d32f0dc-1f15-4276-b887-a66c28a2b5ba")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='8d32f0dc-1f15-4276-b887-a66c28a2b5ba')

    compss_logging(
        "Lemonade task 8d32f0dc-1f15-4276-b887-a66c28a2b5ba started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['new_data_type'] = 'Date/time'
    settings['attributes'] = [u'DTHR']
    settings['new_name'] = [u'DTHR']
    data16 = AttributesChangerOperation(data0, settings, numFrag)

    results = {
        'output data': {'output': data16, 'sample': take_sample(data16)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='8d32f0dc-1f15-4276-b887-a66c28a2b5ba')

    compss_logging(
        "Lemonade task 8d32f0dc-1f15-4276-b887-a66c28a2b5ba completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def transformation_10(compss_session, cached_state, emit_event):
    """ Operation b1f6c7ca-da93-44cb-8154-765e84e83988 """

    compss_logging(
        "Submitting parent task [u'8d32f0dc-1f15-4276-b887-a66c28a2b5ba'] before b1f6c7ca-da93-44cb-8154-765e84e83988")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['8d32f0dc-1f15-4276-b887-a66c28a2b5ba']
    data16, pr_data16 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data16 = parent_result['time']

    compss_logging(
        "Parents completed, submitting b1f6c7ca-da93-44cb-8154-765e84e83988")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='b1f6c7ca-da93-44cb-8154-765e84e83988')

    compss_logging(
        "Lemonade task b1f6c7ca-da93-44cb-8154-765e84e83988 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['functions'] = [
        [u'BINS_5_MINS', "lambda col: group_datetime(col['DTHR'], 300)", '']]
    data13 = TransformOperation(data16, settings, numFrag)

    results = {
        'output data': {'output': data13, 'sample': take_sample(data13)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='b1f6c7ca-da93-44cb-8154-765e84e83988')

    compss_logging(
        "Lemonade task b1f6c7ca-da93-44cb-8154-765e84e83988 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def aggregation_11(compss_session, cached_state, emit_event):
    """ Operation 807b7fab-65d9-40e9-b3a7-5fcd8aef9090 """

    compss_logging(
        "Submitting parent task [u'b1f6c7ca-da93-44cb-8154-765e84e83988'] before 807b7fab-65d9-40e9-b3a7-5fcd8aef9090")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['b1f6c7ca-da93-44cb-8154-765e84e83988']
    data13, pr_data13 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data13 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 807b7fab-65d9-40e9-b3a7-5fcd8aef9090")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='807b7fab-65d9-40e9-b3a7-5fcd8aef9090')

    compss_logging(
        "Lemonade task 807b7fab-65d9-40e9-b3a7-5fcd8aef9090 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['columns'] = [u'BINS_5_MINS', u'COD_LINHA', u'VEIC']
    settings['operation'] = {u'LAT': [u'first'], u'LON': [u'first']}
    settings['aliases'] = {u'LAT': [u'LAT'], u'LON': [u'LON']}
    data10 = AggregationOperation(data13, settings, numFrag)

    results = {
        'output data': {'output': data10, 'sample': take_sample(data10)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='807b7fab-65d9-40e9-b3a7-5fcd8aef9090')

    compss_logging(
        "Lemonade task 807b7fab-65d9-40e9-b3a7-5fcd8aef9090 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def join_12(compss_session, cached_state, emit_event):
    """ Operation 5ce6f958-79c9-440e-a10c-07b78e74f1c8 """

    compss_logging(
        "Submitting parent task [u'ef8c53c4-0c3b-428d-bafa-18d3e82704d5', u'807b7fab-65d9-40e9-b3a7-5fcd8aef9090'] before 5ce6f958-79c9-440e-a10c-07b78e74f1c8")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['ef8c53c4-0c3b-428d-bafa-18d3e82704d5']
    data2, pr_data2 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data2 = parent_result['time']

    parent_result = task_futures['807b7fab-65d9-40e9-b3a7-5fcd8aef9090']
    data10, pr_data10 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data10 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 5ce6f958-79c9-440e-a10c-07b78e74f1c8")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='5ce6f958-79c9-440e-a10c-07b78e74f1c8')

    compss_logging(
        "Lemonade task 5ce6f958-79c9-440e-a10c-07b78e74f1c8 started")

    start = timer()
    numFrag = 16
    params = dict()
    params['option'] = 'inner'
    params['key1'] = [u'BINS_5_MINS', u'CODLINHA', u'CODVEICULO']
    params['key2'] = [u'BINS_5_MINS', u'COD_LINHA', u'VEIC']
    params['case'] = True
    params['suffixes'] = [u'_l', u' _r']
    params['keep_keys'] = False
    data1 = JoinOperation(data2, data10, params, numFrag)

    results = {
        'output data': {'output': data1, 'sample': take_sample(data1)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='5ce6f958-79c9-440e-a10c-07b78e74f1c8')

    compss_logging(
        "Lemonade task 5ce6f958-79c9-440e-a10c-07b78e74f1c8 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def transformation_13(compss_session, cached_state, emit_event):
    """ Operation f44b0ec9-7ff1-4173-92d4-7a43ff91351d """

    compss_logging(
        "Submitting parent task [u'5ce6f958-79c9-440e-a10c-07b78e74f1c8'] before f44b0ec9-7ff1-4173-92d4-7a43ff91351d")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['5ce6f958-79c9-440e-a10c-07b78e74f1c8']
    data1, pr_data1 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data1 = parent_result['time']

    compss_logging(
        "Parents completed, submitting f44b0ec9-7ff1-4173-92d4-7a43ff91351d")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='f44b0ec9-7ff1-4173-92d4-7a43ff91351d')

    compss_logging(
        "Lemonade task f44b0ec9-7ff1-4173-92d4-7a43ff91351d started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['functions'] =  [u'DATE',
            "lambda col: col['DATAUTILIZACAO'].date()",""]]
    data14 = TransformOperation(data1, settings, numFrag)

    results = {
        'output data': {'output': data14, 'sample': take_sample(data14)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='f44b0ec9-7ff1-4173-92d4-7a43ff91351d')

    compss_logging(
        "Lemonade task f44b0ec9-7ff1-4173-92d4-7a43ff91351d completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def aggregation_14(compss_session, cached_state, emit_event):
    """
    Counts
    """

    compss_logging(
        "Submitting parent task [u'f44b0ec9-7ff1-4173-92d4-7a43ff91351d'] before ff5d88f7-54df-491d-b7e7-d1a3427db305")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['f44b0ec9-7ff1-4173-92d4-7a43ff91351d']
    data14, pr_data14 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data14 = parent_result['time']

    compss_logging(
        "Parents completed, submitting ff5d88f7-54df-491d-b7e7-d1a3427db305")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='ff5d88f7-54df-491d-b7e7-d1a3427db305')

    compss_logging(
        "Lemonade task ff5d88f7-54df-491d-b7e7-d1a3427db305 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['columns'] = [u'DATE', u'NUMEROCARTAO']
    settings['operation'] = {u'NUMEROCARTAO': [u'count']}
    settings['aliases'] = {u'NUMEROCARTAO': [u'COUNT']}
    data11 = AggregationOperation(data14, settings, numFrag)

    results = {
        'output data': {'output': data11, 'sample': take_sample(data11)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='ff5d88f7-54df-491d-b7e7-d1a3427db305')

    compss_logging(
        "Lemonade task ff5d88f7-54df-491d-b7e7-d1a3427db305 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def projection_15(compss_session, cached_state, emit_event):
    """ Operation f49606e1-87a1-4a8d-afeb-a92799911fd7 """

    compss_logging(
        "Submitting parent task [u'ff5d88f7-54df-491d-b7e7-d1a3427db305'] before f49606e1-87a1-4a8d-afeb-a92799911fd7")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['ff5d88f7-54df-491d-b7e7-d1a3427db305']
    data11, pr_data11 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data11 = parent_result['time']

    compss_logging(
        "Parents completed, submitting f49606e1-87a1-4a8d-afeb-a92799911fd7")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='f49606e1-87a1-4a8d-afeb-a92799911fd7')

    compss_logging(
        "Lemonade task f49606e1-87a1-4a8d-afeb-a92799911fd7 started")

    start = timer()
    numFrag = 16
    columns = ["NUMEROCARTAO", "COUNT"]
    data19 = SelectOperation(data11, columns, numFrag)

    results = {
        'output projected data': {'output': data19, 'sample': take_sample(data19)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='f49606e1-87a1-4a8d-afeb-a92799911fd7')

    compss_logging(
        "Lemonade task f49606e1-87a1-4a8d-afeb-a92799911fd7 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def filter_selection_16(compss_session, cached_state, emit_event):
    """ Operation b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6 """

    compss_logging(
        "Submitting parent task [u'f49606e1-87a1-4a8d-afeb-a92799911fd7'] before b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['f49606e1-87a1-4a8d-afeb-a92799911fd7']
    data19, pr_data19 = (parent_result['output projected data'][
                         'output'], parent_result['output projected data']['sample'])
    ts_data19 = parent_result['time']

    compss_logging(
        "Parents completed, submitting b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6')

    compss_logging(
        "Lemonade task b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['query'] = "(COUNT >= 2) "
    data4 = FilterOperation(data19, settings, numFrag)

    results = {
        'output data': {'output': data4, 'sample': take_sample(data4)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6')

    compss_logging(
        "Lemonade task b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def join_17(compss_session, cached_state, emit_event):
    """ Operation 1bd8b925-cd52-42bf-86d6-3aef0c2e82fe """

    compss_logging(
        "Submitting parent task [u'b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6', u'f44b0ec9-7ff1-4173-92d4-7a43ff91351d'] before 1bd8b925-cd52-42bf-86d6-3aef0c2e82fe")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6']
    data4, pr_data4 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data4 = parent_result['time']

    parent_result = task_futures['f44b0ec9-7ff1-4173-92d4-7a43ff91351d']
    data14, pr_data14 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data14 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 1bd8b925-cd52-42bf-86d6-3aef0c2e82fe")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='1bd8b925-cd52-42bf-86d6-3aef0c2e82fe')

    compss_logging(
        "Lemonade task 1bd8b925-cd52-42bf-86d6-3aef0c2e82fe started")

    start = timer()
    numFrag = 16
    params = dict()
    params['option'] = 'inner'
    params['key1'] = [u'DATE', u'NUMEROCARTAO']
    params['key2'] = [u'DATE', u'NUMEROCARTAO']
    params['case'] = True
    params['suffixes'] = [u'_l', u' _r']
    params['keep_keys'] = False
    data22 = JoinOperation(data4, data14, params, numFrag)

    results = {
        'output data': {'output': data22, 'sample': take_sample(data22)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='1bd8b925-cd52-42bf-86d6-3aef0c2e82fe')

    compss_logging(
        "Lemonade task 1bd8b925-cd52-42bf-86d6-3aef0c2e82fe completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def projection_18(compss_session, cached_state, emit_event):
    """ Operation a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97 """

    compss_logging(
        "Submitting parent task [u'1bd8b925-cd52-42bf-86d6-3aef0c2e82fe'] before a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['1bd8b925-cd52-42bf-86d6-3aef0c2e82fe']
    data22, pr_data22 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data22 = parent_result['time']

    compss_logging(
        "Parents completed, submitting a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97')

    compss_logging(
        "Lemonade task a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97 started")

    start = timer()
    numFrag = 16
    columns = [
        "CODLINHA",
        "CODVEICULO",
        "BINS_5_MINS",
        "NUMEROCARTAO",
        "DATAUTILIZACAO",
        "LAT",
        "LON"]
    data17 = SelectOperation(data22, columns, numFrag)

    results = {
        'output projected data': {'output': data17, 'sample': take_sample(data17)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97')

    compss_logging(
        "Lemonade task a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def remove_duplicated_rows_19(compss_session, cached_state, emit_event):
    """ Operation 8443ba4f-8bc8-44a7-b507-d7234f024386 """

    compss_logging(
        "Submitting parent task [u'a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97'] before 8443ba4f-8bc8-44a7-b507-d7234f024386")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97']
    data17, pr_data17 = (parent_result['output projected data'][
                         'output'], parent_result['output projected data']['sample'])
    ts_data17 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 8443ba4f-8bc8-44a7-b507-d7234f024386")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='8443ba4f-8bc8-44a7-b507-d7234f024386')

    compss_logging(
        "Lemonade task 8443ba4f-8bc8-44a7-b507-d7234f024386 started")

    start = timer()
    numFrag = 16
    columns = [] 
    data15 = DistinctOperation(data17, columns, numFrag)

    results = {
        'output data': {'output': data15, 'sample': take_sample(data15)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='8443ba4f-8bc8-44a7-b507-d7234f024386')

    compss_logging(
        "Lemonade task 8443ba4f-8bc8-44a7-b507-d7234f024386 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def within_20(compss_session, cached_state, emit_event):
    """ Operation 020bf4e5-77b5-4385-8387-64e0544b4cb1 """

    compss_logging(
        "Submitting parent task [u'8a564cb8-65b4-4d26-a71a-378e3fff0379', u'8443ba4f-8bc8-44a7-b507-d7234f024386'] before 020bf4e5-77b5-4385-8387-64e0544b4cb1")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['8a564cb8-65b4-4d26-a71a-378e3fff0379']
    data23, pr_data23 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data23 = parent_result['time']

    parent_result = task_futures['8443ba4f-8bc8-44a7-b507-d7234f024386']
    data15, pr_data15 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data15 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 020bf4e5-77b5-4385-8387-64e0544b4cb1")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='020bf4e5-77b5-4385-8387-64e0544b4cb1')

    compss_logging(
        "Lemonade task 020bf4e5-77b5-4385-8387-64e0544b4cb1 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['lat_col'] = "LAT"
    settings['long_col'] = "LON"
    settings['attributes'] = [u'CODSECTOR', u'CODBAIRR', u'NOMEBAIR']
    settings['alias'] = '_shp'
    settings['polygon'] = 'points'
    data7 = GeoWithinOperation(data15, data23, settings, numFrag)

    results = {
        'output data': {'output': data7, 'sample': take_sample(data7)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='020bf4e5-77b5-4385-8387-64e0544b4cb1')

    compss_logging(
        "Lemonade task 020bf4e5-77b5-4385-8387-64e0544b4cb1 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def join_21(compss_session, cached_state, emit_event):
    """ Operation d85e4564-63c2-453b-bcb2-3de4d92ea3fe """

    compss_logging(
        "Submitting parent task [u'7458cad9-0a82-487a-b65a-f55a17039757', u'020bf4e5-77b5-4385-8387-64e0544b4cb1'] before d85e4564-63c2-453b-bcb2-3de4d92ea3fe")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['7458cad9-0a82-487a-b65a-f55a17039757']
    data18, pr_data18 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data18 = parent_result['time']

    parent_result = task_futures['020bf4e5-77b5-4385-8387-64e0544b4cb1']
    data7, pr_data7 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data7 = parent_result['time']

    compss_logging(
        "Parents completed, submitting d85e4564-63c2-453b-bcb2-3de4d92ea3fe")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='d85e4564-63c2-453b-bcb2-3de4d92ea3fe')

    compss_logging(
        "Lemonade task d85e4564-63c2-453b-bcb2-3de4d92ea3fe started")

    start = timer()
    numFrag = 16
    params = dict()
    params['option'] = 'inner'
    params['key1'] = [u'CODSECTOR_shp']
    params['key2'] = [u'CODSECTOR']
    params['case'] = True
    params['suffixes'] = [u'_l', u' _r']
    params['keep_keys'] = False
    data9 = JoinOperation(data7, data18, params, numFrag)

    results = {
        'output data': {'output': data9, 'sample': take_sample(data9)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='d85e4564-63c2-453b-bcb2-3de4d92ea3fe')

    compss_logging(
        "Lemonade task d85e4564-63c2-453b-bcb2-3de4d92ea3fe completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def aggregation_22(compss_session, cached_state, emit_event):
    """ Operation 5dc06491-85c0-4bf9-9e6c-d87bc50382e6 """

    compss_logging(
        "Submitting parent task [u'd85e4564-63c2-453b-bcb2-3de4d92ea3fe'] before 5dc06491-85c0-4bf9-9e6c-d87bc50382e6")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['d85e4564-63c2-453b-bcb2-3de4d92ea3fe']
    data9, pr_data9 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data9 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 5dc06491-85c0-4bf9-9e6c-d87bc50382e6")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='5dc06491-85c0-4bf9-9e6c-d87bc50382e6')

    compss_logging(
        "Lemonade task 5dc06491-85c0-4bf9-9e6c-d87bc50382e6 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['columns'] = [u'NUMEROCARTAO', u'DATE']
    settings['operation'] = {u'CODBAIRR_shp': ['first', 'last'],
                             u'LON': ['first', 'last'],
                             u'LAT': ['first', 'last'],
                             u'BA_002': ['first', 'last'],
                             u'BA_005': ['first', 'last'],
                             u'P1_001': ['first', 'last'],
                             u'NOMEBAIR_shp': ['first', 'last'],
                             u'DATAUTILIZACAO': ['first', 'last'],
			                 u'CODLINHA': ['first']
                             }
    settings['aliases'] = {u'CODBAIRR_shp': ['o_neight_code', 'd_neight_code'],
                           'LON': ['o_lon', 'd_lon'],
                           'LAT': ['o_lat', 'd_lat'],
                           'BA_002': ['o_num_pop', 'd_num_pop'],
                           'BA_005': ['o_renda', 'd_renda'],
                           'P1_001': ['o_num_alfa', 'd_num_alfa'],
                           'NOMEBAIR_shp': ['o_neight_name', 'd_neight_name'],
                           'DATAUTILIZACAO': ['o_timestamp', 'd_timestamp'],
                           'CODLINHA': ['codlinha']
                           }
    data21 = AggregationOperation(data9, settings, numFrag)

    results = {
        'output data': {'output': data21, 'sample': take_sample(data21)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='5dc06491-85c0-4bf9-9e6c-d87bc50382e6')

    compss_logging(
        "Lemonade task 5dc06491-85c0-4bf9-9e6c-d87bc50382e6 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def sort_23(compss_session, cached_state, emit_event):
    """
    Sort by total
    """

    compss_logging(
        "Submitting parent task [u'5dc06491-85c0-4bf9-9e6c-d87bc50382e6'] before 8bab8ae7-e445-465a-bb99-fbf2173b4120")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['5dc06491-85c0-4bf9-9e6c-d87bc50382e6']
    data21, pr_data21 = (parent_result['output data'][
                         'output'], parent_result['output data']['sample'])
    ts_data21 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 8bab8ae7-e445-465a-bb99-fbf2173b4120")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='8bab8ae7-e445-465a-bb99-fbf2173b4120')

    compss_logging(
        "Lemonade task 8bab8ae7-e445-465a-bb99-fbf2173b4120 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['columns'] = [u'o_timestamp']
    settings['ascending'] = [True]
    settings['algorithm'] = 'bitonic'
    data8 = SortOperation(data21, settings, numFrag)

    results = {
        'output data': {'output': data8, 'sample': take_sample(data8)},
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='8bab8ae7-e445-465a-bb99-fbf2173b4120')

    compss_logging(
        "Lemonade task 8bab8ae7-e445-465a-bb99-fbf2173b4120 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def data_writer_24(compss_session, cached_state, emit_event):
    """ Operation 0206aa3d-3b8e-4bf1-890a-bb245882de81 """

    compss_logging(
        "Submitting parent task [u'8bab8ae7-e445-465a-bb99-fbf2173b4120'] before 0206aa3d-3b8e-4bf1-890a-bb245882de81")

    # Next we wait for the dependencies to complete

    parent_result = task_futures['8bab8ae7-e445-465a-bb99-fbf2173b4120']
    data8, pr_data8 = (parent_result['output data'][
                       'output'], parent_result['output data']['sample'])
    ts_data8 = parent_result['time']

    compss_logging(
        "Parents completed, submitting 0206aa3d-3b8e-4bf1-890a-bb245882de81")
    emit_event(name='update task', message='Task running',
               status='RUNNING',
               identifier='0206aa3d-3b8e-4bf1-890a-bb245882de81')

    compss_logging(
        "Lemonade task 0206aa3d-3b8e-4bf1-890a-bb245882de81 started")

    start = timer()
    numFrag = 16
    settings = dict()
    settings['filename'] = '/output_peoplepath.csv'
    settings['mode'] = 'overwrite'
    settings['header'] = True
    settings['format'] = 'CSV'
    SaveHDFSOperation(data8, settings)
    output_data_1 = None

    results = {
    }
    emit_event(name='update task', message='Task completed',
               status='COMPLETED',
               identifier='0206aa3d-3b8e-4bf1-890a-bb245882de81')

    compss_logging(
        "Lemonade task 0206aa3d-3b8e-4bf1-890a-bb245882de81 completed")
    time_elapsed = timer() - start
    results['time'] = time_elapsed
    return results


def main(compss_session, cached_state, emit_event):
    """ Run generated code """

    start = time.time()

    session_start_time = time.time()
    task_futures['45163792-5078-4af9-ab2d-d3357f1cdd8e'] = data_reader_0(
        compss_session, cached_state, emit_event)
    task_futures['7458cad9-0a82-487a-b65a-f55a17039757'] = data_reader_1(
        compss_session, cached_state, emit_event)
    task_futures['3d9260ef-f037-45fa-b339-da280ed00c0a'] = change_attribute_2(
        compss_session, cached_state, emit_event)
    task_futures['4360919c-d6b4-4d3b-b9b2-f44037c19c15'] = read_shapefile_3(
        compss_session, cached_state, emit_event)
    task_futures['8a564cb8-65b4-4d26-a71a-378e3fff0379'] = filter_selection_4(
        compss_session, cached_state, emit_event)
    task_futures['a8d54724-6087-4a18-8d3b-f53ebecb4328'] = transformation_5(
        compss_session, cached_state, emit_event)
    task_futures['e512da41-bd81-4213-a4f7-19113652a3c3'] = projection_6(
        compss_session, cached_state, emit_event)
    task_futures['ef8c53c4-0c3b-428d-bafa-18d3e82704d5'] = sort_7(
        compss_session, cached_state, emit_event)
    task_futures['86794e6d-5e98-4608-9633-ceaa4a79e3a3'] = data_reader_8(
        compss_session, cached_state, emit_event)
    task_futures['8d32f0dc-1f15-4276-b887-a66c28a2b5ba'] = change_attribute_9(
        compss_session, cached_state, emit_event)
    task_futures['b1f6c7ca-da93-44cb-8154-765e84e83988'] = transformation_10(
        compss_session, cached_state, emit_event)
    task_futures['807b7fab-65d9-40e9-b3a7-5fcd8aef9090'] = aggregation_11(
        compss_session, cached_state, emit_event)
    task_futures['5ce6f958-79c9-440e-a10c-07b78e74f1c8'] = join_12(
        compss_session, cached_state, emit_event)
    task_futures['f44b0ec9-7ff1-4173-92d4-7a43ff91351d'] = transformation_13(
        compss_session, cached_state, emit_event)
    task_futures['ff5d88f7-54df-491d-b7e7-d1a3427db305'] = aggregation_14(
        compss_session, cached_state, emit_event)
    task_futures['f49606e1-87a1-4a8d-afeb-a92799911fd7'] = projection_15(
        compss_session, cached_state, emit_event)
    task_futures['b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6'] = filter_selection_16(
        compss_session, cached_state, emit_event)
    task_futures['1bd8b925-cd52-42bf-86d6-3aef0c2e82fe'] = join_17(
        compss_session, cached_state, emit_event)
    task_futures['a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97'] = projection_18(
        compss_session, cached_state, emit_event)
    task_futures['8443ba4f-8bc8-44a7-b507-d7234f024386'] = remove_duplicated_rows_19(
        compss_session, cached_state, emit_event)
    task_futures['020bf4e5-77b5-4385-8387-64e0544b4cb1'] = within_20(
        compss_session, cached_state, emit_event)
    task_futures['d85e4564-63c2-453b-bcb2-3de4d92ea3fe'] = join_21(
        compss_session, cached_state, emit_event)
    task_futures['5dc06491-85c0-4bf9-9e6c-d87bc50382e6'] = aggregation_22(
        compss_session, cached_state, emit_event)
    task_futures['8bab8ae7-e445-465a-bb99-fbf2173b4120'] = sort_23(
        compss_session, cached_state, emit_event)
    task_futures['0206aa3d-3b8e-4bf1-890a-bb245882de81'] = data_writer_24(
        compss_session, cached_state, emit_event)

    end = time.time()
    print("{}\t{}".format(end - start, end - session_start_time))
    return {
        'status': 'OK',
        'message': 'Execution defined',
        '45163792-5078-4af9-ab2d-d3357f1cdd8e':
        task_futures['45163792-5078-4af9-ab2d-d3357f1cdd8e'],
            '7458cad9-0a82-487a-b65a-f55a17039757':
                task_futures['7458cad9-0a82-487a-b65a-f55a17039757'],
            '3d9260ef-f037-45fa-b339-da280ed00c0a':
                task_futures['3d9260ef-f037-45fa-b339-da280ed00c0a'],
            '4360919c-d6b4-4d3b-b9b2-f44037c19c15':
                task_futures['4360919c-d6b4-4d3b-b9b2-f44037c19c15'],
            '8a564cb8-65b4-4d26-a71a-378e3fff0379':
                task_futures['8a564cb8-65b4-4d26-a71a-378e3fff0379'],
            'a8d54724-6087-4a18-8d3b-f53ebecb4328':
                task_futures['a8d54724-6087-4a18-8d3b-f53ebecb4328'],
            'e512da41-bd81-4213-a4f7-19113652a3c3':
                task_futures['e512da41-bd81-4213-a4f7-19113652a3c3'],
            'ef8c53c4-0c3b-428d-bafa-18d3e82704d5':
                task_futures['ef8c53c4-0c3b-428d-bafa-18d3e82704d5'],
            '86794e6d-5e98-4608-9633-ceaa4a79e3a3':
                task_futures['86794e6d-5e98-4608-9633-ceaa4a79e3a3'],
            '8d32f0dc-1f15-4276-b887-a66c28a2b5ba':
                task_futures['8d32f0dc-1f15-4276-b887-a66c28a2b5ba'],
            'b1f6c7ca-da93-44cb-8154-765e84e83988':
                task_futures['b1f6c7ca-da93-44cb-8154-765e84e83988'],
            '807b7fab-65d9-40e9-b3a7-5fcd8aef9090':
                task_futures['807b7fab-65d9-40e9-b3a7-5fcd8aef9090'],
            '5ce6f958-79c9-440e-a10c-07b78e74f1c8':
                task_futures['5ce6f958-79c9-440e-a10c-07b78e74f1c8'],
            'f44b0ec9-7ff1-4173-92d4-7a43ff91351d':
                task_futures['f44b0ec9-7ff1-4173-92d4-7a43ff91351d'],
            'ff5d88f7-54df-491d-b7e7-d1a3427db305':
                task_futures['ff5d88f7-54df-491d-b7e7-d1a3427db305'],
            'f49606e1-87a1-4a8d-afeb-a92799911fd7':
                task_futures['f49606e1-87a1-4a8d-afeb-a92799911fd7'],
            'b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6':
                task_futures['b5f059d6-4ce1-4a7a-b25b-88dfb56c6cf6'],
            '1bd8b925-cd52-42bf-86d6-3aef0c2e82fe':
                task_futures['1bd8b925-cd52-42bf-86d6-3aef0c2e82fe'],
            'a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97':
                task_futures['a4d0edd4-95f2-47c1-9ff7-6d3f48e59f97'],
            '8443ba4f-8bc8-44a7-b507-d7234f024386':
                task_futures['8443ba4f-8bc8-44a7-b507-d7234f024386'],
            '020bf4e5-77b5-4385-8387-64e0544b4cb1':
                task_futures['020bf4e5-77b5-4385-8387-64e0544b4cb1'],
            'd85e4564-63c2-453b-bcb2-3de4d92ea3fe':
                task_futures['d85e4564-63c2-453b-bcb2-3de4d92ea3fe'],
            '5dc06491-85c0-4bf9-9e6c-d87bc50382e6':
                task_futures['5dc06491-85c0-4bf9-9e6c-d87bc50382e6'],
            '8bab8ae7-e445-465a-bb99-fbf2173b4120':
                task_futures['8bab8ae7-e445-465a-bb99-fbf2173b4120'],
            '0206aa3d-3b8e-4bf1-890a-bb245882de81':
                task_futures['0206aa3d-3b8e-4bf1-890a-bb245882de81'],
    }


def dummy_emit_event(room, namespace):
    def _dummy_emit_event(name, message, status, identifier, **kwargs):
        return None
    return _dummy_emit_event

compss_session = "COMPSs"

main(compss_session, {}, dummy_emit_event(room=-1, namespace='/none'))
