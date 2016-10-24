import logging


def or_(left_side, right_side):
    if type(left_side) is not dict or \
            type(right_side) is not dict:
        logging.error("Wrong OR operation arguments!. Make sure you are comparing columns with something.")
        return {}
    if not left_side or not right_side:
        return {}
    return {
        'type': 'or',
        'left_side': left_side,
        'right_side': right_side
    }
