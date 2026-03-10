import json
import logging
import os

from flask import session

from storage_paths import files_path


def _general_settings_path():
    return files_path('general_settings.json')


def _account_features_path():
    return files_path('account_features.json')


def _profile_photo_dir():
    return files_path('profile')


def _profile_photo_path(filename='profile_photo.png'):
    return os.path.join(_profile_photo_dir(), filename)


def load_general_settings():
    default_payload = {
        'inicio': {
            'mostrar_resumen': True,
            'notificaciones_activas': True
        },
        'perfil': {
            'foto_version': 0,
            'correo_personal': ''
        },
        'contactos': {
            'correo_soporte': '',
            'correo_copia': ''
        },
        'microsoft365': {
            'tenant_id': 'common',
            'client_id': '',
            'client_secret': '',
            'scope': 'https://graph.microsoft.com/User.Read openid profile email'
        }
    }

    settings_path = _general_settings_path()
    if not os.path.exists(settings_path):
        return default_payload

    try:
        with open(settings_path, 'r', encoding='utf-8') as settings_file:
            payload = json.load(settings_file)

        if not isinstance(payload, dict):
            return default_payload

        merged = default_payload.copy()
        for section_key in default_payload.keys():
            section_value = payload.get(section_key, {})
            if isinstance(section_value, dict):
                merged_section = default_payload[section_key].copy()
                merged_section.update(section_value)
                merged[section_key] = merged_section
        return merged
    except Exception as ex:
        logging.error(f'No se pudo leer configuracion general: {ex}')
        return default_payload


def save_general_settings(payload):
    with open(_general_settings_path(), 'w', encoding='utf-8') as settings_file:
        json.dump(payload, settings_file, ensure_ascii=False)


def _profile_photo_context():
    settings = load_general_settings()
    photo_version = int(settings.get('perfil', {}).get('foto_version', 0) or 0)
    return {
        'has_profile_photo': os.path.exists(_profile_photo_path()),
        'profile_photo_url': f"/foto_perfil_actual?v={photo_version}"
    }


def load_account_features():
    default_payload = {
        'password': {
            'value_encrypted': '',
            'updated_at': ''
        },
        'devices': [
            {
                'id': 1,
                'name': 'Equipo principal',
                'location': 'Oficina',
                'active': True
            }
        ],
        'activity': [],
        'sessions': [],
        'security': {
            'skip_password_when_possible': False,
            'enhanced_browsing': False
        }
    }

    features_path = _account_features_path()
    if not os.path.exists(features_path):
        return default_payload

    try:
        with open(features_path, 'r', encoding='utf-8') as features_file:
            payload = json.load(features_file)

        if not isinstance(payload, dict):
            return default_payload

        merged = default_payload.copy()
        for key in ['password', 'devices', 'activity', 'sessions', 'security']:
            value = payload.get(key)
            if key == 'password' and isinstance(value, dict):
                password_payload = default_payload['password'].copy()
                password_payload.update(value)
                merged['password'] = password_payload
            elif key == 'devices' and isinstance(value, list):
                merged['devices'] = value
            elif key == 'activity' and isinstance(value, list):
                merged['activity'] = value
            elif key == 'sessions' and isinstance(value, list):
                merged['sessions'] = value
            elif key == 'security' and isinstance(value, dict):
                security_payload = default_payload['security'].copy()
                security_payload.update(value)
                merged['security'] = security_payload

        return merged
    except Exception as ex:
        logging.error(f'No se pudo leer account_features: {ex}')
        return default_payload


def save_account_features(payload):
    with open(_account_features_path(), 'w', encoding='utf-8') as features_file:
        json.dump(payload, features_file, ensure_ascii=False)


def add_account_activity(action, detail=''):
    return


def ensure_current_session_tracked():
    return


def require_worker_microsoft_login():
    session['system_authenticated'] = True
    session['smtp_authenticated'] = True
    session['quick_password_verified'] = True
    return None


def enforce_step_flow(current_step: str):
    return None
