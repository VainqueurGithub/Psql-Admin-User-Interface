
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import psycopg2
import qrcode
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from tkcalendar import DateEntry
from datetime import datetime
import os
from PIL import Image, ImageDraw

# =====================================================
# 1. DATABASE CONFIGURATION
# =====================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH = os.path.join(BASE_DIR, "media")
LOGO_PATH = os.path.join(LOGO_PATH, "logo_dfgf.png")
PHOTO_FOLDER = os.path.join(BASE_DIR, "media/employee_photos")
GORILLA_BG_PATH = os.path.join(BASE_DIR, "media/gorilla.jpg")
DB_CONFIG = {
    "host": "",
    "database": "",
    "user": "",
    "password": "",
    "port": ""
}


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def get_employees():
    conn = connect_db()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT id_personne, nom, prenom, poste, departement, id_site_affectation
            FROM ressources_humaines.employe
            ORDER BY id_personne;
        """)

        employees = cur.fetchall()
        cur.close()

        return employees

    finally:
        conn.close()

def create_gorilla_watermark(image_path, opacity=0.12):

    img = Image.open(image_path).convert("RGBA")

    alpha = img.getchannel("A")
    alpha = alpha.point(lambda p: int(p * opacity))
    img.putalpha(alpha)

    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    return ImageReader(buffer)

def rounded_photo(image_path, radius=25):

    img = Image.open(image_path).convert("RGBA")

    # Resize and crop to portrait dimensions
    width, height = 400, 400

    img_ratio = img.width / img.height
    target_ratio = width / height

    if img_ratio > target_ratio:
        new_height = height
        new_width = int(height * img_ratio)
    else:
        new_width = width
        new_height = int(width / img_ratio)

    img = img.resize(
        (new_width, new_height),
        Image.Resampling.LANCZOS
    )

    left = (new_width - width) // 2
    top = (new_height - height) // 2

    img = img.crop((
        left, top,
        left + width,
        top + height
    ))

    # Create rounded mask
    mask = Image.new("L", (width, height), 0)

    draw = ImageDraw.Draw(mask)

    draw.ellipse((0, 0, width, height),fill=255)

    img.putalpha(mask)

    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    return ImageReader(buffer)
# =====================================================
# 2. QR CODE GENERATION
# =====================================================

def generate_qr(matricule, name, first_name, site, program, valid, position):

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=8,
        border=2
    )

    qr.add_data(f"EMP:{matricule}/{name} {first_name} affecté (e) à {site} au programme: {program} en tant que {position}. Cette carte est valide jusqu'au {valid}")
    qr.make(fit=True)

    img = qr.make_image(fill_color="black",back_color="white").convert("RGB")

    output = BytesIO()
    img.save(output, format="PNG")
    output.seek(0)

    return output


# =====================================================
# 3. GENERATE EMPLOYEE SERVICE CARD PDF
# =====================================================

def generate_cards(employees, output_file, expiry_date):

    # Exact card dimensions: 82 x 50 mm
    card_width = 82 * mm
    card_height = 50 * mm

    pdf = canvas.Canvas(output_file,pagesize=(card_width, card_height))

    for emp in employees:

        matricule, name, first_name, position, program, site = emp
        
        # Background
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, card_width, card_height, fill=1, stroke=0)

        # Header
        pdf.setFillColorRGB(46/255, 125/255, 50/255)
        pdf.rect(0, 39 * mm,card_width, 11 * mm,fill=1, stroke=0)

        for ext in [".jpg", ".jpeg", ".png"]:
            candidate = os.path.join(PHOTO_FOLDER,f"{matricule}{ext}")

            if os.path.isfile(candidate):
                photo_path = candidate
                break

        # Organisation logo - LEFT
        if os.path.exists(photo_path):
            pdf.drawImage(rounded_photo(photo_path),2 * mm,40 * mm,width=9 * mm,height=9 * mm,preserveAspectRatio=True,anchor='c',mask='auto')

        pdf.setFillColor(colors.white)
        pdf.setFont("Helvetica-Bold", 9)

        pdf.drawCentredString(card_width / 2,45 * mm,"THE DIAN FOSSEY GORILLA FUND")
        pdf.setFont("Helvetica", 6.5)
        pdf.drawCentredString(card_width / 2,41.5 * mm,"CARTE OFFICIELLE DE SERVICE DE L'AGENT")


        # EMPLOYEE PHOTO - RIGHT

        if os.path.exists(LOGO_PATH):
            pdf.drawImage(rounded_photo(LOGO_PATH),71 * mm,40 * mm,width=9 * mm,height=9 * mm,preserveAspectRatio=True,anchor='c',mask='auto')

        
        # Employee information
        pdf.setFillColor(colors.black)

        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(4 * mm,32 * mm,str(name or "") + " " + str(first_name or ""))

        pdf.setFont("Helvetica", 7)

        pdf.drawString(4 * mm, 26 * mm,f"Matricule: {matricule}")

        pdf.drawString(4 * mm, 21 * mm,f"Position: {position or ''}")


        # Gorilla background watermark
    
        HEADER_HEIGHT = 11 * mm


        # Additional details
        if program:
            pdf.setFont("Helvetica", 6)
            pdf.drawString(4 * mm, 16 * mm,str(program)[:45])


        # Draw gorilla image below the header
        if os.path.isfile(GORILLA_BG_PATH):
            gorilla_watermark = create_gorilla_watermark(GORILLA_BG_PATH,opacity=0.15)
        
            pdf.drawImage(gorilla_watermark,0,0,width=card_width,height=card_height - HEADER_HEIGHT,preserveAspectRatio=False,mask='auto')
        
            # Draw green header on top
            pdf.setFillColorRGB(34/255, 139/255, 34/255)
            pdf.rect(0,card_width - HEADER_HEIGHT,card_width,HEADER_HEIGHT,fill=1,stroke=0)
        
        # QR code
        qr_buffer = generate_qr(matricule, name, first_name, site, program, expiry_date, position)

        pdf.drawImage(ImageReader(qr_buffer),60 * mm,11 * mm,width=18 * mm,height=18 * mm,preserveAspectRatio=True,mask='auto')

        # Site of affectation and expiration date
        pdf.setFont("Helvetica-Bold", 7)
        pdf.setFillColor(colors.black)

        pdf.drawString(4 * mm,8 * mm,f"Affecté (e) à: {site or ''}")

        pdf.drawRightString(78 * mm,8 * mm,f" Jusqu'au: {expiry_date or ''}")

        # Footer
        pdf.setStrokeColorRGB(0.09, 0.22, 0.36)
        pdf.line(4 * mm, 7 * mm,78 * mm, 7 * mm)

        pdf.setFont("Helvetica-Bold", 6)
        pdf.setFillColorRGB(0.09, 0.22, 0.36)

        pdf.drawCentredString(card_width / 2,3.5 * mm,"Grauer's Gorilla Research and Conservation Program")

        pdf.drawCentredString(card_width / 2,1 * mm,"Republique Démocratique du Congo")

        pdf.showPage()

    pdf.save()


# =====================================================
# 4. TKINTER APPLICATION
# =====================================================

class EmployeeApp:

    def __init__(self, root):

        self.root = root
        self.root.title("Employee Service Card Generator")
        self.root.geometry("950x600")

        self.employees = []

        self.create_interface()
        self.load_employees()

    def create_interface(self):

        title = tk.Label(
            self.root,
            text="EMPLOYEE SERVICE CARD GENERATOR",
            font=("Arial", 18, "bold"),
            fg="#173B5E"
        )

        title.pack(pady=15)

        # Employee table
        columns = (
            "matricule",
            "name",
            "first_name",
            "position",
            "details"
        )

        self.tree = ttk.Treeview(
            self.root,
            columns=columns,
            show="headings",
            height=17,
            selectmode="extended"
        )

        headings = {
            "matricule": "Matricule",
            "name": "Family name",
            "first_name": "First name",
            "position": "Position",
            "details": "Details"
        }

        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=160)

        self.tree.pack(
            fill="both",
            expand=True,
            padx=20,
            pady=10
        )

        # Expiration date section
        date_frame = tk.Frame(self.root)
        date_frame.pack(pady=10)

        tk.Label(date_frame,text="Card Expiration Date:",font=("Arial", 11, "bold")).pack(side="left", padx=5)
        self.expiration_date = DateEntry(date_frame,width=15,background="#173B5E",foreground="white",borderwidth=2,date_pattern="dd/mm/yyyy")
        self.expiration_date.pack(side="left", padx=5)
        
        # Buttons
        buttons = tk.Frame(self.root)
        buttons.pack(pady=15)

        tk.Button(buttons,text="Refresh Employees",command=self.load_employees,width=20).pack(side="left", padx=5)
        tk.Button(buttons,text="Generate Selected Cards",command=self.generate_selected,width=23,bg="#173B5E",fg="white").pack(side="left", padx=5)
        tk.Button(buttons,text="Generate All Cards",command=self.generate_all,width=20,bg="#287A50",fg="white").pack(side="left", padx=5)

    # Load employees from PostgreSQL
    def load_employees(self):

        try:
            self.employees = get_employees()

            for item in self.tree.get_children():
                self.tree.delete(item)

            for emp in self.employees:
                self.tree.insert("", "end", values=emp)

        except Exception as e:
            messagebox.showerror(
                "Database Error",
                str(e)
            )

    # Generate selected employee cards
    def generate_selected(self):

        selected = self.tree.selection()

        if not selected:
            messagebox.showwarning(
                "Selection",
                "Please select at least one employee."
            )
            return

        employees = [
            self.tree.item(item)["values"]
            for item in selected
        ]
        expiry_date = self.expiration_date.get()
        self.export_pdf(employees, expiry_date)

    # Generate cards for all employees
    def generate_all(self):

        if not self.employees:
            messagebox.showwarning(
                "Warning",
                "No employee records found."
            )
            return
        expiry_date = self.expiration_date.get()
        self.export_pdf(self.employees, expiry_date)

    # Export PDF
    def export_pdf(self, employees, expiry_date):

        output_file = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf")],
            initialfile="employee_service_cards.pdf"
        )

        if not output_file:
            return

        try:
            generate_cards(employees, output_file, expiry_date)

            messagebox.showinfo(
                "Success",
                f"{len(employees)} employee cards generated successfully."
            )

        except Exception as e:
            messagebox.showerror(
                "PDF Error",
                str(e)
            )


# =====================================================
# 5. RUN APPLICATION
# =====================================================

if __name__ == "__main__":

    root = tk.Tk()

    app = EmployeeApp(root)

    root.mainloop()
