import re
from datetime import datetime

def extract_file_name(file_content):
    file_content = file_content.strip()
    lines = file_content.split('\n')
    title = lines[0]
    return title

def extract_position(file_content):
    pass

def extract_class_code(file_content):
    pass

def extract_start_date(file_content):
    pass

def extract_end_date(file_content):
    pass

def extract_salary(file_content):
    pass

def extract_requirements(file_content):
    pass

def extract_notes(file_content):
    try:
        notes_match = re.search(r'(NOTES?):(.*?)(?=DUTIES)', file_content, re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None
        return notes
    except Exception as e:
        raise ValueError(f'Error extracting notes: {str(e)}')

def extract_duties(file_content):
    pass

def extract_selection(file_content):
    pass

def extract_experience_length(file_content):
    pass

def extract_education_length(file_content):
    pass

def extract_school_type(file_content):
    pass

def extract_application_location(file_content):
    pass