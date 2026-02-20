from VoucherService import VoucherService
from datetime import datetime

# Crear instancia del servicio
voucher_service = VoucherService()

# Datos de prueba
datos_cliente = {
    'fecha': datetime.now().strftime('%d/%m/%Y'),
    'descripcion': 'Pago de servicios eléctricos',
    'monto': 1250.50,
    'referencia': 'CLI-00123456',
    'operacion_numero': 'OP-2026-001234',
    'email': 'u212prac01@distriluz.com.pe',
    'asiento': 'ASI-2026-5678'
}

# Generar voucher
print("Generando voucher de prueba...")
filepath = voucher_service.generar_voucher_cliente(datos_cliente)
print(f"✓ Voucher generado exitosamente en: {filepath}")
