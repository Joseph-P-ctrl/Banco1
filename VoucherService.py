import os
from datetime import datetime
import pandas as pd

class VoucherService:
    def __init__(self, output_dir='files/vouchers'):
        self.output_dir = output_dir
        # Crear directorio si no existe
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generar_voucher_cliente(self, datos_cliente):
        """
        Genera un voucher en HTML para un cliente específico.
        
        Args:
            datos_cliente: dict con las claves:
                - fecha: fecha de la operación
                - descripcion: descripción de la operación
                - monto: monto de la operación
                - referencia: referencia del cliente
                - nombre_cliente: nombre del cliente (opcional)
                - operacion_numero: número de operación
                - email: email del cliente
                - asiento: número de asiento (opcional)
        
        Returns:
            str: ruta del archivo HTML generado
        """
        # Sanitizar el nombre del archivo
        email = datos_cliente.get('email', 'sin_email')
        referencia = datos_cliente.get('referencia', 'sin_ref')
        safe_filename = f"voucher_{referencia}_{email.replace('@', '_').replace('.', '_')}.html"
        filepath = os.path.join(self.output_dir, safe_filename)
        fecha_emision = datetime.now().strftime("%d/%m/%Y %H:%M")
        descripcion = str(datos_cliente.get('descripcion', 'Pago de servicios'))
        if len(descripcion) > 120:
            descripcion = descripcion[:120] + "..."

        monto = datos_cliente.get('monto', 0)
        monto_str = f"S/ {monto:,.2f}" if isinstance(monto, (int, float)) else str(monto)

        referencia_valor = str(datos_cliente.get('referencia', 'N/A'))
        fecha_operacion_valor = str(datos_cliente.get('fecha', 'N/A'))
        operacion_numero_valor = str(datos_cliente.get('operacion_numero', 'N/A'))

        html_content = f"""<!DOCTYPE html>
<html lang=\"es\">
<head>
    <meta charset=\"UTF-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
    <title>Voucher de Abono - {referencia_valor}</title>
    <style>
        body {{
            margin: 0;
            background: #f3f5f7;
            font-family: Arial, Helvetica, sans-serif;
            color: #2c3e50;
        }}
        .page {{
            max-width: 820px;
            margin: 20px auto;
            background: #ffffff;
            border: 1px solid #d0d8e0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .header {{
            background: #1e5da8;
            color: #ffffff;
            padding: 24px 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .brand-title {{
            margin: 0;
            font-size: 34px;
            font-weight: 700;
            line-height: 1;
        }}
        .brand-subtitle {{
            margin: 6px 0 0;
            font-size: 16px;
            font-weight: 700;
            letter-spacing: .5px;
        }}
        .emision {{
            font-size: 12px;
            text-align: right;
        }}
        .content {{
            padding: 28px 30px;
        }}
        .section-title {{
            margin: 0 0 20px;
            color: #1e5da8;
            font-size: 22px;
            font-weight: 700;
        }}
        .box {{
            background: #e8f1f8;
            border: 1px solid #d0d8e0;
            border-radius: 8px;
            padding: 16px 18px;
        }}
        .box-title {{
            margin: 0 0 12px;
            color: #1e5da8;
            font-size: 15px;
            font-weight: 700;
            border-bottom: 2px solid #1e5da8;
            padding-bottom: 8px;
        }}
        .row {{
            margin: 8px 0;
            font-size: 14px;
        }}
        .row strong {{
            display: inline-block;
            width: 155px;
        }}
        .monto {{
            margin-top: 20px;
            border: 2px solid #1e5da8;
            border-radius: 8px;
            padding: 14px 18px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: #f8f9fa;
        }}
        .monto-label {{
            color: #1e5da8;
            font-weight: 700;
            font-size: 20px;
        }}
        .monto-value {{
            color: #1e5da8;
            font-weight: 800;
            font-size: 34px;
        }}
        .footer {{
            text-align: center;
            color: #7f8c8d;
            font-size: 12px;
            padding: 18px 30px 24px;
            border-top: 1px solid #e1e6eb;
        }}
        .footer .brand {{
            color: #1e5da8;
            font-weight: 700;
            margin-top: 6px;
            display: block;
        }}
    </style>
</head>
<body>
    <div class=\"page\">
        <div class=\"header\">
            <div>
                <h1 class=\"brand-title\">Ensa</h1>
                <p class=\"brand-subtitle\">ABONO RECIBIDO</p>
            </div>
            <div class=\"emision\">Fecha de emisión: {fecha_emision}</div>
        </div>
        <div class=\"content\">
            <h2 class=\"section-title\">DATOS DE ABONO RECIBIDO</h2>
            <div class=\"box\">
                <h3 class=\"box-title\">REFERENCIA DEL CLIENTE</h3>
                <div class=\"row\"><strong>Referencia:</strong> {referencia_valor}</div>
                <div class=\"row\"><strong>Fecha Operación:</strong> {fecha_operacion_valor}</div>
                <div class=\"row\"><strong>Nº Operación:</strong> {operacion_numero_valor}</div>
                <div class=\"row\"><strong>Descripción:</strong> {descripcion}</div>
            </div>
            <div class=\"monto\">
                <div class=\"monto-label\">MONTO A CONFIRMAR:</div>
                <div class=\"monto-value\">{monto_str}</div>
            </div>
        </div>
        <div class=\"footer\">
            <div>Este documento es una confirmación de abono recibido.</div>
            <div>Para mayor información, contacte con el área de recaudación al correo recaudacionensa@distriluz.com.pe</div>
            <span class=\"brand\">ENSA © 2026</span>
        </div>
    </div>
</body>
</html>
"""

        with open(filepath, 'w', encoding='utf-8') as html_file:
            html_file.write(html_content)
        
        return filepath
    
    def generar_vouchers_desde_dataframe(self, df_movimientos, clientes_email_map=None):
        """
        Genera vouchers para todos los registros sin voucher en un DataFrame.
        
        Args:
            df_movimientos: DataFrame con los movimientos procesados
            clientes_email_map: dict con mapeo de referencia a email
        
        Returns:
            list: lista de rutas de archivos generados con sus emails
        """
        vouchers_generados = []
        
        # Buscar registros sin voucher
        voucher_cols = [col for col in df_movimientos.columns 
                       if 'documento' in col.lower() or 'voucher' in col.lower() or 'asiento' in col.lower()]
        
        if not voucher_cols:
            # Si no hay columna de voucher, procesar todos los registros
            df_sin_voucher = df_movimientos
        else:
            voucher_col = voucher_cols[0]
            df_sin_voucher = df_movimientos[
                df_movimientos[voucher_col].isna() | 
                (df_movimientos[voucher_col].astype(str).str.strip() == '')
            ]
        
        for index, row in df_sin_voucher.iterrows():
            # Extraer datos del row
            referencia = str(row.get('Referencia', row.get('referencia', f'REF-{index}')))
            
            # Buscar email
            email = None
            if 'Correo' in row and pd.notna(row['Correo']) and str(row['Correo']).strip():
                email = str(row['Correo']).strip()
            elif 'Correos' in row and pd.notna(row['Correos']) and str(row['Correos']).strip():
                email = str(row['Correos']).split(',')[0].strip()
            elif clientes_email_map and referencia.upper() in clientes_email_map:
                email = clientes_email_map[referencia.upper()]
            
            if not email:
                continue  # No generar voucher si no hay email
            
            # Preparar datos del voucher
            datos_cliente = {
                'fecha': row.get('Fecha', row.get('fecha', datetime.now().strftime('%d/%m/%Y'))),
                'descripcion': row.get('Descripción operación', row.get('descripcion', 'Operación bancaria')),
                'monto': row.get('Monto', row.get('monto', 0)),
                'referencia': referencia,
                'operacion_numero': row.get('Operación - Número', row.get('operacion', 'N/A')),
                'email': email,
                'asiento': row.get('Asientos', row.get('asiento', None))
            }
            
            # Convertir fecha a string si es necesario
            if isinstance(datos_cliente['fecha'], pd.Timestamp):
                datos_cliente['fecha'] = datos_cliente['fecha'].strftime('%d/%m/%Y')
            
            # Generar el voucher
            try:
                filepath = self.generar_voucher_cliente(datos_cliente)
                vouchers_generados.append({
                    'email': email,
                    'referencia': referencia,
                    'filepath': filepath,
                    'monto': datos_cliente['monto']
                })
            except Exception as e:
                print(f"Error generando voucher para {email}: {str(e)}")
                continue
        
        return vouchers_generados
    
    def limpiar_vouchers_antiguos(self, dias=7):
        """
        Elimina vouchers más antiguos de X días.
        """
        import time
        now = time.time()
        cutoff = now - (dias * 86400)
        
        for filename in os.listdir(self.output_dir):
            filepath = os.path.join(self.output_dir, filename)
            if os.path.isfile(filepath):
                if os.path.getmtime(filepath) < cutoff:
                    try:
                        os.remove(filepath)
                    except Exception as e:
                        print(f"No se pudo eliminar {filepath}: {e}")
