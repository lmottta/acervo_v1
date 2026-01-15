
from fpdf import FPDF
import pandas as pd

def test_pdf():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="Hello World", ln=1, align="C")
    
    try:
        # Attempt legacy usage
        res = pdf.output(dest='S')
        print(f"Output type: {type(res)}")
        if isinstance(res, str):
            print("Output is string (Legacy behavior)")
        else:
            print("Output is not string")
    except Exception as e:
        print(f"Legacy output failed: {e}")
        
    try:
        # Attempt modern usage
        res2 = pdf.output()
        print(f"Modern output type: {type(res2)}")
    except Exception as e:
        print(f"Modern output failed: {e}")

if __name__ == "__main__":
    test_pdf()
