from typing import List, Dict


def get_person_json(person_ids: List[str], person_names: List[str]) -> List[Dict[str, str]]:
    """Формирование данных по персонам в виде {id: ..., name: ...}"""
    if person_ids is None:
        return [{}]
    return [{'id': person_id, 'name': person_name} 
            for person_id, person_name in zip(person_ids, person_names)]
