import customtkinter as ctk
from tkinter import filedialog, messagebox
import os
import csv
import threading
import re 

# --- IMPORTS DE SCRIPTS ---
import lusha_script
import apollo_script
import apollo_org
import lusha_org

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.stop_event = threading.Event()
        self.current_thread = None

        # --- Definición de UI (Sin cambios) ---
        self.title("Herramienta de Extracción de Datos v3.8")
        self.geometry("1100x850") 
        self.minsize(1100,850)
        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")

        self.tab_view = ctk.CTkTabview(self, corner_radius=5)
        self.tab_view.pack(pady=5, padx=5, fill="both", expand=True)
        self.tab1 = self.tab_view.add("Extracción de Contactos")

        # Layout Pestaña 1 (Sin cambios)
        self.tab1.grid_columnconfigure(0, weight=1)
        self.tab1.grid_rowconfigure(1, weight=3) 
        self.tab1.grid_rowconfigure(3, weight=2) 
        
        self.api_frame = ctk.CTkFrame(self.tab1, corner_radius=5)
        self.api_frame.grid(row=0, column=0, pady=5, padx=0, sticky="ew")
        
        self.middle_frame = ctk.CTkFrame(self.tab1, corner_radius=0, fg_color="transparent")
        self.middle_frame.grid(row=1, column=0, pady=5, padx=0, sticky="nsew")
        self.middle_frame.grid_columnconfigure(0, weight=1)
        self.middle_frame.grid_columnconfigure(1, weight=1)
        self.middle_frame.grid_rowconfigure(0, weight=1) 
        
        self.action_frame = ctk.CTkFrame(self.tab1, corner_radius=10)
        self.action_frame.grid(row=2, column=0, pady=5, padx=0, sticky="ew")

        self.cancel_frame = ctk.CTkFrame(self.tab1, corner_radius=10)
        self.cancel_frame.grid(row=3, column=0, pady=5, padx=0, sticky="ew")
        
        self.console_frame = ctk.CTkFrame(self.tab1, corner_radius=10)
        self.console_frame.grid(row=4, column=0, pady=(5,0), padx=0, sticky="nsew")

        # --- Widgets ---
        self._create_widgets()

    # --- Creación de Widgets Pestaña 1 (Sin cambios) ---
    def _create_widgets(self):
        # API Keys (Sin cambios)
        self.apollo_api_label = ctk.CTkLabel(self.api_frame, text="API Key Apollo:")
        self.apollo_api_label.pack(side="left", padx=(5, 5), pady=5)
        self.apollo_api_entry = ctk.CTkEntry(self.api_frame, placeholder_text="...", show="*", width=250)
        self.apollo_api_entry.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        
        self.lusha_api_label = ctk.CTkLabel(self.api_frame, text="API Key Lusha:")
        self.lusha_api_label.pack(side="left", padx=(5, 5), pady=5)
        self.lusha_api_entry = ctk.CTkEntry(self.api_frame, placeholder_text="...", show="*", width=250)
        self.lusha_api_entry.pack(side="left", fill="x", expand=True, padx=(5, 5), pady=5)
        
        # Países (Con mapeo: Español para UI, Inglés para API)
        countries_panel = ctk.CTkFrame(self.middle_frame, corner_radius=5)
        countries_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10)) 
        ctk.CTkLabel(countries_panel, text="Seleccionar Países", font=("Arial", 14, "bold")).pack(pady=10)
        scroll_frame = ctk.CTkScrollableFrame(countries_panel)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Mapeo: Nombre en Español (UI) -> Nombre en Inglés (API)
        paises_mapeo = {
            "Norteamérica": {
                "Estados Unidos": "United States",
                "Canadá": "Canada",
                "México": "Mexico"
            },
            "Centroamérica": {
                "Belice": "Belize",
                "Costa Rica": "Costa Rica",
                "El Salvador": "El Salvador",
                "Guatemala": "Guatemala",
                "Honduras": "Honduras",
                "Nicaragua": "Nicaragua",
                "Panamá": "Panama"
            },
            "Suramérica": {
                "Argentina": "Argentina",
                "Bolivia": "Bolivia",
                "Brasil": "Brazil",
                "Chile": "Chile",
                "Colombia": "Colombia",
                "Ecuador": "Ecuador",
                "Guyana": "Guyana",
                "Paraguay": "Paraguay",
                "Perú": "Peru",
                "Surinam": "Suriname",
                "Uruguay": "Uruguay",
                "Venezuela": "Venezuela"
            },
            "Caribe": {
                "Antigua y Barbuda": "Antigua and Barbuda",
                "Bahamas": "Bahamas",
                "Barbados": "Barbados",
                "Bermudas": "Bermuda",
                "Dominica": "Dominica",
                "República Dominicana": "Dominican Republic",
                "Granada": "Grenada",
                "Guadalupe": "Guadeloupe",
                "Haití": "Haiti",
                "Jamaica": "Jamaica",
                "Martinica": "Martinique",
                "Montserrat": "Montserrat",
                "Puerto Rico": "Puerto Rico",
                "San Cristóbal y Nieves": "Saint Kitts and Nevis",
                "Santa Lucía": "Saint Lucia",
                "San Vicente y las Granadinas": "Saint Vincent and the Grenadines",
                "Trinidad y Tobago": "Trinidad and Tobago",
                "Islas Turcas y Caicos": "Turks and Caicos Islands"
            }
        }
        
        # Almacenar mapeo para usar en start_process
        self.paises_mapeo = paises_mapeo
        
        self.country_checkboxes = {}
        for region, paises_dict in paises_mapeo.items():
            ctk.CTkLabel(scroll_frame, text=region, font=("Arial", 11, "bold")).pack(anchor="w", pady=(5, 5), padx=5)
            for pais_es, pais_en in paises_dict.items():
                self.country_checkboxes[pais_es] = ctk.CTkCheckBox(scroll_frame, text=pais_es, checkbox_width=14, checkbox_height=14, font=("Arial", 11))
                self.country_checkboxes[pais_es].pack(anchor="w", padx=10, pady=2)
        
        # Archivos (Sin cambios)
        files_panel = ctk.CTkFrame(self.middle_frame, corner_radius=10)
        files_panel.grid(row=0, column=1, sticky="nsew", padx=(5, 0)) 
        self.cargos_entry = self._create_file_selector(files_panel, "Archivo CSV de Cargos", self.browse_cargos_file)
        self.empresas_entry = self._create_file_selector(files_panel, "Archivo CSV de Empresas", self.browse_empresas_file)
        self.id_org_entry = self._create_file_selector(files_panel, "Archivo CSV de Id Organizaciones", self.browse_id_org_file)
        self.output_entry = self._create_folder_selector(files_panel, "Carpeta de Destino para Resultados", self.browse_output_folder)
        
        # Botones (Sin cambios)
        self.apollo_contact_button = ctk.CTkButton(self.action_frame, text="Apollo Contactos", command=lambda: self.start_process("APOLLO_CONTACT"), height=30, font=("Arial", 12, "bold"), fg_color="#867903", hover_color="#E0CC11")
        self.apollo_contact_button.pack(side="left", fill="x", expand=True, padx=(2, 2), pady=2)
        self.apollo_org_button = ctk.CTkButton(self.action_frame, text="Apollo Organizaciones", command=lambda: self.start_process("APOLLO_ORG"), height=30, font=("Arial", 12, "bold"), fg_color="#867903", hover_color="#E0CC11")
        self.apollo_org_button.pack(side="left", fill="x", expand=True, padx=(2, 2), pady=2)
        self.lusha_contact_button = ctk.CTkButton(self.action_frame, text="Lusha Contactos", command=lambda: self.start_process("LUSHA_CONTACT"), height=30, font=("Arial", 12, "bold"), fg_color="#53045F", hover_color="#9E06B6")
        self.lusha_contact_button.pack(side="left", fill="x", expand=True, padx=(2, 2), pady=2)
        self.lusha_org_button = ctk.CTkButton(self.action_frame, text="Lusha Organizaciones", command=lambda: self.start_process("LUSHA_ORG"), height=30, font=("Arial", 12, "bold"), fg_color="#53045F", hover_color="#9E06B6")
        self.lusha_org_button.pack(side="left", fill="x", expand=True, padx=(2, 2), pady=2)
        # Signal script está vacío - botón deshabilitado
        # self.signal_contact_button = ctk.CTkButton(self.action_frame, text="SignalHere Contactos", command=lambda: self.start_process("SIGNAL_CONTACT"), height=30, font=("Arial", 12, "bold"), fg_color="#083588", hover_color="#0A46B6")
        # self.signal_contact_button.pack(side="left", fill="x", expand=True, padx=(2, 2), pady=2)
        
        self.cancel_button = ctk.CTkButton(self.cancel_frame, text="Cancelar", command=self.cancel_process, height=30, font=("Arial", 14, "bold"), fg_color="#781A07", hover_color="#B32003", state="disabled")
        self.cancel_button.pack(fill="x", padx=5, pady=5)
        
        # Consola (Sin cambios)
        ctk.CTkLabel(self.console_frame, text="Consola de Ejecución", font=("Arial", 12, "bold")).pack(pady=(5, 5))
        self.console_textbox = ctk.CTkTextbox(self.console_frame, state="disabled", font=("Consolas", 12))
        self.console_textbox.pack(fill="both", expand=True, padx=10, pady=(5, 5))


    # ==========================================================
    # --- FUNCIONES DE LA UI (Ayudantes, Loggers, Browsers) ---
    # ==========================================================

    def log(self, message):
        """Escribe un mensaje en la consola de la Pestaña 1."""
        self.console_textbox.configure(state="normal")
        self.console_textbox.insert("end", message + "\n")
        self.console_textbox.configure(state="disabled")
        self.console_textbox.see("end")

    # ============================================================
    # --- FUNCIONES DE SEGURIDAD ---
    # ============================================================

    def _mask_api_key(self, api_key):
        """Enmascara API key para logs seguros"""
        if not api_key or len(api_key) < 8:
            return "***"
        return f"{api_key[:4]}...{api_key[-4:]}"

    def _validate_api_key(self, api_key, api_name):
        """Valida que una API key no esté vacía"""
        if not api_key or not api_key.strip():
            self.log(f"❌ ERROR: API Key {api_name} es requerida")
            return False
        return True

    def _safe_log_process_start(self, process_type, api_key, details=""):
        """Loguea inicio de proceso sin exponer credenciales"""
        masked_key = self._mask_api_key(api_key)
        self.log(f"\n--- Iniciando Proceso: {process_type} ---")
        self.log(f"🔐 API Key: {masked_key}")
        if details:
            self.log(f"📊 {details}")



    # (Funciones browse_... Pestaña 1)
    def browse_cargos_file(self):
        path = filedialog.askopenfilename(title="Archivo de Cargos", filetypes=[("Archivos CSV", "*.csv")])
        if path: self.cargos_entry.delete(0, "end"); self.cargos_entry.insert(0, path)

    def browse_empresas_file(self):
        path = filedialog.askopenfilename(title="Archivo de Empresas", filetypes=[("Archivos CSV", "*.csv")])
        if path: self.empresas_entry.delete(0, "end"); self.empresas_entry.insert(0, path)

    def browse_id_org_file(self):
        path = filedialog.askopenfilename(title="Archivo Id Organizaciones", filetypes=[("Archivos CSV", "*.csv")])
        if path: self.id_org_entry.delete(0, "end"); self.id_org_entry.insert(0, path)
        
    def browse_output_folder(self):
        path = filedialog.askdirectory(title="Carpeta de Destino")
        if path: self.output_entry.delete(0, "end"); self.output_entry.insert(0, path)


    
    




    # (Funciones _create_..._selector sin cambios)
    def _create_file_selector(self, parent, label_text, command):
        # Esta función ahora devuelve la 'entry' para que podamos referenciarla
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(pady=10, padx=10, fill="x") # Reducido pady de 15 a 10
        ctk.CTkLabel(frame, text=label_text, font=("Arial", 14, "bold")).pack(anchor="w")
        entry = ctk.CTkEntry(frame, placeholder_text="Seleccionar archivo...")
        entry.pack(side="left", fill="x", expand=True, pady=5, padx=(0, 10))
        ctk.CTkButton(frame, text="Examinar", width=100, command=command).pack(side="left")
        return entry

    def _create_folder_selector(self, parent, label_text, command):
        # Esta función ahora devuelve la 'entry' para que podamos referenciarla
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(pady=10, padx=10, fill="x")
        ctk.CTkLabel(frame, text=label_text, font=("Arial", 14, "bold")).pack(anchor="w")
        entry = ctk.CTkEntry(frame, placeholder_text="Seleccionar carpeta...")
        entry.pack(side="left", fill="x", expand=True, pady=5, padx=(0, 10))
        ctk.CTkButton(frame, text="Examinar", width=100, command=command).pack(side="left")
        return entry
    
    def leer_csv_lista(self, filepath):
            """
            Lee la primera columna de un CSV con validación de seguridad.
            - Valida que el archivo existe y es accesible
            - Limita tamaño máximo (10MB)
            - Limita número de líneas (10,000)
            - Sanitiza valores
            """
            self.log(f"Leyendo archivo: {os.path.basename(filepath)}...")

            # Validar que el archivo existe
            if not os.path.isfile(filepath):
                self.log(f"❌ ERROR: Archivo no encontrado")
                raise FileNotFoundError(f"Archivo no encontrado")

            # Validar extensión
            if not filepath.lower().endswith('.csv'):
                self.log(f"❌ ERROR: Solo se permiten archivos CSV")
                raise ValueError("Solo se permiten archivos CSV")

            # Validar tamaño (máx 10MB)
            file_size = os.path.getsize(filepath)
            if file_size > 10 * 1024 * 1024:
                self.log(f"❌ ERROR: Archivo demasiado grande ({file_size / 1024 / 1024:.1f}MB)")
                raise ValueError("Archivo demasiado grande (máx 10MB)")

            # Leer con límites
            max_lines = 10000
            lines_read = 0

            try:
                with open(filepath, mode='r', encoding='latin-1') as f: 
                    reader = csv.reader(f)
                    try:
                        next(reader)  # Omite encabezado
                    except StopIteration:
                        return [] 

                    result = []
                    for row in reader:
                        if lines_read >= max_lines:
                            self.log(f"⚠️ Límite de {max_lines} líneas alcanzado")
                            break

                        if row and row[0].strip():
                            # Sanitizar: máx 500 caracteres
                            value = row[0].strip()[:500]
                            result.append(value)
                            lines_read += 1

                    self.log(f"✅ Leídas {len(result)} líneas")
                    return result

            except UnicodeDecodeError:
                # Fallback en caso de que sea un UTF-8 real
                try:
                    with open(filepath, mode='r', encoding='utf-8-sig') as f:
                        reader = csv.reader(f)
                        next(reader)
                        result = []
                        for row in reader:
                            if lines_read >= max_lines:
                                break
                            if row and row[0].strip():
                                value = row[0].strip()[:500]
                                result.append(value)
                                lines_read += 1
                        return result
                except Exception as e:
                    self.log(f"❌ ERROR: No se pudo leer el archivo")
                    raise
            except Exception as e:
                self.log(f"❌ ERROR al leer archivo CSV: {str(e)[:100]}")
                raise


    # --- FUNCIONES DE CONTROL DE HILOS (Threads) ---

    def toggle_buttons(self, is_running: bool):
        """
        (NUEVA FUNCIÓN AUXILIAR)
        Habilita o deshabilita los botones de acción de la Pestaña 1.
        """
        state = "disabled" if is_running else "normal"
        self.apollo_contact_button.configure(state=state)
        self.apollo_org_button.configure(state=state)
        self.lusha_contact_button.configure(state=state)
        self.lusha_org_button.configure(state=state)
        # Signal button está deshabilitado - no configurar
        # self.signal_contact_button.configure(state=state)
        
        cancel_state = "normal" if is_running else "disabled"
        self.cancel_button.configure(text="Cancelar", state=cancel_state)

    def monitor_thread(self, thread):
        """Monitorea el hilo de la Pestaña 1."""
        if thread.is_alive():
            self.after(100, self.monitor_thread, thread)
        else:
            self.on_process_finished()

    def on_process_finished(self):
        """
        (REFACTORIZADO)
        Se llama cuando un hilo de la Pestaña 1 termina.
        """
        self.log("...Proceso detenido o finalizado...")
        self.toggle_buttons(is_running=False) # <- USA LA NUEVA FUNCIÓN
        self.stop_event.clear()

    def cancel_process(self):
        """Cancela el proceso actual"""
        self.log("🛑 Cancelando proceso...")
        self.stop_event.set()
        self.cancel_button.configure(text="Cancelando...", state="disabled")
        
        # Esperar a que el thread termine (máximo 5 segundos)
        if hasattr(self, 'current_thread') and self.current_thread:
            self.current_thread.join(timeout=5)
        
        # Restaurar estado después de cancelar
        self.after(500, self.on_process_finished)
    




    # ==========================================================
    # --- COMANDOS DE BOTONES (Delegan la lógica) ---
    # ==========================================================

    # --- Pestaña 1: Extracción (Sin cambios) ---
    def start_process(self, process_type: str):
        """
        (FUNCIÓN PRINCIPAL REFACTORIZADA)
        Valida y despacha la tarea correcta según el botón presionado.
        """
        self.stop_event.clear()
        
        # 1. Obtener todos los valores de la UI
        # Convertir nombres de países de español a inglés
        paises_es_seleccionados = [pais for pais, cb in self.country_checkboxes.items() if cb.get()]
        
        # VALIDACIÓN: Al menos un país debe estar seleccionado
        if not paises_es_seleccionados:
            self.log("❌ ERROR: Debes seleccionar al menos un país")
            return
        
        paises_en = []
        for pais_es in paises_es_seleccionados:
            # Buscar el nombre en inglés en el mapeo
            for region_dict in self.paises_mapeo.values():
                if pais_es in region_dict:
                    paises_en.append(region_dict[pais_es])
                    break
        
        ui_values = {
            "apollo_api": self.apollo_api_entry.get(),
            "lusha_api": self.lusha_api_entry.get(),
            "cargos_file": self.cargos_entry.get(),
            "empresas_file": self.empresas_entry.get(),
            "id_org_file": self.id_org_entry.get(),
            "output_folder": self.output_entry.get(),
            "paises": paises_en  # Usar nombres en inglés
        }

        target_func = None
        args = ()
        validation_ok = False

        try:
            # VALIDACIÓN: Carpeta de destino debe existir y ser escribible
            output_folder = ui_values["output_folder"]
            if not output_folder:
                self.log("❌ ERROR: Debes seleccionar una carpeta de destino")
                return
            
            if not os.path.isdir(output_folder):
                self.log("❌ ERROR: La carpeta de destino no existe")
                return
            
            if not os.access(output_folder, os.W_OK):
                self.log("❌ ERROR: No hay permisos de escritura en la carpeta de destino")
                return
            
            # VALIDACIÓN: Verificar espacio en disco
            try:
                import shutil
                stat = shutil.disk_usage(output_folder)
                free_mb = stat.free / (1024 * 1024)
                if free_mb < 100:
                    self.log(f"❌ ERROR: Espacio en disco insuficiente ({free_mb:.1f}MB disponible, 100MB requerido)")
                    return
            except Exception as e:
                self.log(f"⚠️ Advertencia: No se pudo verificar espacio en disco: {e}")
            
            # 2. Despachador: Valida y prepara la tarea
            if process_type == "APOLLO_CONTACT":
                # Requeridos: Api Key Apollo, Cargos, empresas, paises, carpeta de destino
                required = [ui_values["apollo_api"], ui_values["cargos_file"], ui_values["empresas_file"], ui_values["paises"], ui_values["output_folder"]]
                if not all(required):
                    self.log("❌ ERROR: Para 'Apollo Contactos' se requiere: API Key Apollo, Archivo Cargos, Archivo Empresas, Países y Carpeta de Destino.")
                    return

                empresas = self.leer_csv_lista(ui_values["empresas_file"])
                cargos = self.leer_csv_lista(ui_values["cargos_file"])
                if not empresas or not cargos:
                    self.log("❌ ERROR: El archivo de empresas o cargos está vacío (después del encabezado).")
                    return

                self._safe_log_process_start("APOLLO_CONTACT", ui_values["apollo_api"], f"{len(empresas)} empresas y {len(cargos)} cargos")
                
                target_func = apollo_script.run
                args = (ui_values["apollo_api"], empresas, cargos, ui_values["paises"], ui_values["output_folder"], self.log, self.stop_event)
                validation_ok = True

            elif process_type == "APOLLO_ORG":
                # Requeridos: Api Key Apollo, id Organizaciones, carpeta de destino
                required = [ui_values["apollo_api"], ui_values["id_org_file"], ui_values["output_folder"]]
                if not all(required):
                    self.log("❌ ERROR: Para 'Apollo Organizaciones' se requiere: API Key Apollo, Archivo Id Organizaciones y Carpeta de Destino.")
                    return
                
                self._safe_log_process_start("APOLLO_ORG", ui_values["apollo_api"], f"Archivo: {os.path.basename(ui_values['id_org_file'])}")

                target_func = apollo_org.run # Usa el script refactorizado
                args = (ui_values["apollo_api"], ui_values["id_org_file"], ui_values["output_folder"], self.log, self.stop_event)
                validation_ok = True

            elif process_type == "LUSHA_CONTACT":
                # Requeridos: Api Key lusha, Cargos, empresas, paises, carpeta de destino
                required = [ui_values["lusha_api"], ui_values["cargos_file"], ui_values["empresas_file"], ui_values["paises"], ui_values["output_folder"]]
                if not all(required):
                    self.log("❌ ERROR: Para 'Lusha Contactos' se requiere: API Key Lusha, Archivo Cargos, Archivo Empresas, Países y Carpeta de Destino.")
                    return

                empresas = self.leer_csv_lista(ui_values["empresas_file"])
                cargos = self.leer_csv_lista(ui_values["cargos_file"])
                if not empresas or not cargos:
                    self.log("❌ ERROR: El archivo de empresas o cargos está vacío.")
                    return

                self._safe_log_process_start("LUSHA_CONTACT", ui_values["lusha_api"], f"{len(empresas)} empresas y {len(cargos)} cargos")

                target_func = lusha_script.run
                args = (ui_values["lusha_api"], empresas, cargos, ui_values["paises"], ui_values["output_folder"], self.log, self.stop_event)
                validation_ok = True

            elif process_type == "LUSHA_ORG":
                # Requeridos: Api Key lusha, id Organizaciones, carpeta de destino
                if not lusha_org:
                    self.log(f"❌ ERROR: El script 'lusha_org.py' no se pudo importar. No se puede ejecutar {process_type}.")
                    return
                
                required = [ui_values["lusha_api"], ui_values["id_org_file"], ui_values["output_folder"]]
                if not all(required):
                    self.log("❌ ERROR: Para 'Lusha Organizaciones' se requiere: API Key Lusha, Archivo Id Organizaciones y Carpeta de Destino.")
                    return

                self._safe_log_process_start("LUSHA_ORG", ui_values["lusha_api"], f"Archivo: {os.path.basename(ui_values['id_org_file'])}")

                target_func = lusha_org.run # Asumiendo que existe
                args = (ui_values["lusha_api"], ui_values["id_org_file"], ui_values["output_folder"], self.log, self.stop_event)
                validation_ok = True

            elif process_type == "SIGNAL_ORG":
                self.log(f"ℹ️ La función '{process_type}' no está implementada todavía.")
                # No hacer nada
                return

            else:
                self.log(f"⚠️ ADVERTENCIA: Tipo de proceso desconocido: {process_type}")
                return

        except Exception as e:
            self.log(f"❌ ERROR FATAL al preparar el proceso: {e}")
            import traceback
            self.log(traceback.format_exc())
            return
        
        # 3. Si la validación fue exitosa, lanzar el hilo
        if validation_ok and target_func:
            self.toggle_buttons(is_running=True)
            thread = threading.Thread(target=target_func, args=args)
            self.current_thread = thread  # Guardar referencia para cancelación
            thread.start()
            self.monitor_thread(thread)
        else:
            # Este log es por si algo muy raro pasa
            self.log("ℹ️ Proceso no iniciado. Revise los errores de validación.")



        



if __name__ == "__main__":
    app = App()
    app.mainloop()

