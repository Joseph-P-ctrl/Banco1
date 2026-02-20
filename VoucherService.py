import os
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas
import pandas as pd

class VoucherService:
    def __init__(self, output_dir='files/vouchers'):
        self.output_dir = output_dir
        # Crear directorio si no existe
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generar_voucher_cliente(self, datos_cliente):
        """
        Genera un voucher en PDF para un cliente específico.
        
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
            str: ruta del archivo PDF generado
        """
        # Sanitizar el nombre del archivo
        email = datos_cliente.get('email', 'sin_email')
        referencia = datos_cliente.get('referencia', 'sin_ref')
        safe_filename = f"voucher_{referencia}_{email.replace('@', '_').replace('.', '_')}.pdf"
        filepath = os.path.join(self.output_dir, safe_filename)
        
        # Crear el PDF con tamaño A4
        c = canvas.Canvas(filepath, pagesize=A4)
        width, height = A4
        
        # Colores corporativos modernos
        color_header = colors.HexColor('#1e5da8')  # Azul corporativo
        color_light_blue = colors.HexColor('#e8f1f8')  # Azul claro para fondos
        color_text_dark = colors.HexColor('#2c3e50')
        color_border = colors.HexColor('#d0d8e0')
        
        # ==== ENCABEZADO PRINCIPAL ====
        c.setFillColor(color_header)
        c.rect(0, height - 120, width, 120, fill=True, stroke=False)
        
        # Logo DISTRILUZ (izquierda)
        logo_path = 'static/images/logos.jpeg'
        if os.path.exists(logo_path):
            try:
                c.drawImage(logo_path, 40, height - 105, width=100, height=80, preserveAspectRatio=True, mask='auto')
            except:
                pass
        
        # Título y fecha (derecha)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 24)
        c.drawString(160, height - 50, "Ensa")
        
        
        c.setFont("Helvetica-Bold", 13)
        c.drawString(160, height - 88, "ABONO RECIBIDO")
        
        # Fecha de emisión
        c.setFont("Helvetica", 10)
        fecha_emision = datetime.now().strftime("%d/%m/%Y %H:%M")
        c.drawRightString(width - 40, height - 50, f"Fecha de emisión: {fecha_emision}")
        
        y_position = height - 160
        
        # ==== TÍTULO DEL DOCUMENTO ====
        c.setFillColor(color_header)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y_position, "DATOS DE ABONO RECIBIDO")
        
        y_position -= 35
        
        # ==== REFERENCIA DEL CLIENTE CON DATOS DE OPERACIÓN (Caja consolidada) ====
        box_y = y_position - 125
        c.setFillColor(color_light_blue)
        c.setStrokeColor(color_border)
        c.setLineWidth(1)
        c.roundRect(40, box_y, width - 80, 135, 5, fill=True, stroke=True)
        
        c.setFillColor(color_header)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y_position, "REFERENCIA DEL CLIENTE")
        
        y_position -= 20
        c.setStrokeColor(color_header)
        c.setLineWidth(1.5)
        c.line(50, y_position, width - 50, y_position)
        
        y_position -= 22
        c.setFillColor(color_text_dark)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(50, y_position, "Referencia:")
        c.setFont("Helvetica", 9)
        c.drawString(170, y_position, str(datos_cliente.get('referencia', 'N/A')))
        
        y_position -= 20
        c.setFont("Helvetica-Bold", 9)
        c.drawString(50, y_position, "Fecha Operación:")
        c.setFont("Helvetica", 9)
        c.drawString(170, y_position, str(datos_cliente.get('fecha', 'N/A')))
        
        y_position -= 20
        c.setFont("Helvetica-Bold", 9)
        c.drawString(50, y_position, "Nº Operación:")
        c.setFont("Helvetica", 9)
        c.drawString(170, y_position, str(datos_cliente.get('operacion_numero', 'N/A')))
        
        y_position -= 20
        c.setFont("Helvetica-Bold", 9)
        c.drawString(50, y_position, "Descripción:")
        c.setFont("Helvetica", 9)
        texto_desc = str(datos_cliente.get('descripcion', 'Pago de servicios'))
        if len(texto_desc) > 55:
            texto_desc = texto_desc[:55] + "..."
        c.drawString(170, y_position, texto_desc)
        
        y_position -= 35
        
        # ==== MONTO A CONFIRMAR (Caja destacada) ====
        c.setFillColor(color_header)
        c.setStrokeColor(color_header)
        c.setLineWidth(2)
        c.roundRect(40, y_position - 55, width - 80, 65, 8, fill=False, stroke=True)
        
        c.setFillColor(colors.HexColor('#f8f9fa'))
        c.roundRect(41, y_position - 54, width - 82, 63, 7, fill=True, stroke=False)
        
        c.setFillColor(color_header)
        c.setFont("Helvetica-Bold", 13)
        c.drawString(60, y_position - 20, "MONTO A CONFIRMAR:")
        
        c.setFont("Helvetica-Bold", 26)
        monto = datos_cliente.get('monto', 0)
        monto_str = f"S/ {monto:,.2f}" if isinstance(monto, (int, float)) else str(monto)
        c.drawRightString(width - 60, y_position - 25, monto_str)
        
        # ==== PIE DE PÁGINA ====
        y_position = 70
        c.setFillColor(colors.HexColor('#95a5a6'))
        c.setFont("Helvetica", 8)
        c.drawCentredString(width/2, y_position, "Este documento es una confirmación de abono recibido.")
        c.drawCentredString(width/2, y_position - 12, "Para mayor información, contacte con el área de recaudación al correo")
        c.drawCentredString(width/2, y_position - 24, "recaudacionensa@distriluz.com.pe")
        
        c.setFillColor(color_header)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(width/2, y_position - 44, " ENSA © 2026")
        
        # Guardar el PDF
        c.save()
        
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
