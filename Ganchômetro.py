import customtkinter as ctk
import sqlite3
from tkinter import ttk, messagebox, filedialog 
import datetime
import os
import json
from PIL import Image, ImageTk, UnidentifiedImageError 
import webbrowser
import matplotlib
matplotlib.use('TkAgg') 
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.ticker import MaxNLocator
import sys
import random 
import traceback
import shutil

DB_NAME = "dbdbrina_stats.db"
APP_NAME = "Ganchômetro"
APP_VERSION = "1.0.0" 
IMAGE_ASSETS_PATH = "assets/"
KILLER_PORTRAITS_PATH = os.path.join(IMAGE_ASSETS_PATH, "portraits", "killers")
ITEM_ICONS_PATH = os.path.join(IMAGE_ASSETS_PATH, "icons", "items") 
LOGO_FILENAME = "logo.png"

PORTRAIT_SIZE_BUTTON_KILLER = (90, 90)
KILLER_CARD_WIDTH = PORTRAIT_SIZE_BUTTON_KILLER[0] + 20
KILLER_CARD_HEIGHT = PORTRAIT_SIZE_BUTTON_KILLER[1] + 45
MAP_ITEM_CARD_WIDTH = 180 
MAP_ITEM_CARD_HEIGHT = 55 
PORTRAIT_SIZE_STATS = (64, 64) 
ITEM_ICON_SIZE_STATS = (48, 48) 
LOGO_TARGET_HEIGHT = 75 
GAME_MODES = ["Solo", "Dupla", "Trio", "SWF"]
COLOR_BACKGROUND = "#101010" 
COLOR_TEXT = "#F0F0F0" 
COLOR_TEXT_SUBTLE = "#A0A0A0" 
COLOR_PRIMARY_RED = "#8B0000" 
COLOR_SECONDARY_RED = "#B22222" 
COLOR_FRAME_BG = "#1C1C1C" 
COLOR_BUTTON_PRIMARY = COLOR_PRIMARY_RED
COLOR_BUTTON_SECONDARY = "#333333" 
COLOR_BUTTON_HOVER_PRIMARY = COLOR_SECONDARY_RED
COLOR_BUTTON_HOVER_SECONDARY = "#454545"
COLOR_BORDER_SELECTED = COLOR_SECONDARY_RED
COLOR_PROGRESS_GREEN = "#00C851" 
COLOR_PROGRESS_ORANGE = "#ffbb33" 
COLOR_PROGRESS_RED_BAR = "#CC0000" 
SECRET_CODE = "stopassole"

def _get_log_path(log_filename):
    try:
        if hasattr(sys, '_MEIPASS'): 
            base_path = os.path.dirname(sys.executable) 
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        
        if not os.access(base_path, os.W_OK):
            base_path = os.getcwd() 
        
        return os.path.join(base_path, log_filename)
    except Exception:
        return os.path.join(os.getcwd(), log_filename)


def _log_error(error_message, log_filename="ganchometro_error_log.txt"):
    full_log_path = _get_log_path(log_filename)
    print(f"Tentando gravar log em: {full_log_path}") 
    try:
        with open(full_log_path, "a", encoding="utf-8") as f: 
            f.write(f"--- {datetime.datetime.now()} ---\n")
            f.write(error_message)
            f.write("\n\n")
        print(f"Log de erro salvo em: {full_log_path}")
    except Exception as log_e:
        print(f"FALHA CRÍTICA AO SALVAR LOG: Não foi possível salvar o log de erro em '{full_log_path}': {log_e}")
        print(f"Mensagem de erro original que tentou ser logada:\n{error_message}")

def get_app_base_dir():
    if hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS 
    else:
        return os.path.dirname(os.path.abspath(__file__))

def get_db_path():
    if hasattr(sys, '_MEIPASS'):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, DB_NAME)

def initialize_database():
    db_path = get_db_path()
    db_existed = os.path.exists(db_path)
    
    criar_tabelas() 

    if not db_existed:
        print(f"Banco de dados '{DB_NAME}' não existia e foi criado em: {db_path}")
        conn_temp = None
        try:
            conn_temp, cursor_temp = conectar_db() 
            cursor_temp.execute("DELETE FROM matches")
            cursor_temp.execute("DELETE FROM match_teammates")
            conn_temp.commit()
            print(f"Tabelas 'matches' e 'match_teammates' garantidas como vazias no banco de dados recém-criado.")
        except sqlite3.Error as e:
            _log_error(f"Erro ao limpar tabelas de partidas no banco de dados recém-criado: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        finally:
            if conn_temp:
                conn_temp.close()
    else:
        print(f"Usando banco de dados existente em: {db_path}")


def conectar_db():
    db_path = get_db_path() 
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    return conn, cursor

def get_id_by_name(table_name, item_name, conn_cursor_tuple):
    conn, cursor = conn_cursor_tuple
    try:
        cursor.execute(f"SELECT id FROM {table_name} WHERE name = ?", (item_name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"Erro ao buscar ID para '{item_name}' em '{table_name}': {e}")
        _log_error(f"Erro SQLite em get_id_by_name para '{item_name}' em '{table_name}': {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        return None

def _add_db_columns_if_not_exists(conn, cursor):
    try:
        cursor.execute("PRAGMA table_info(matches)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'game_mode' not in columns:
            cursor.execute("ALTER TABLE matches ADD COLUMN game_mode TEXT")
        if 'jhones_sedex' not in columns:
            cursor.execute("ALTER TABLE matches ADD COLUMN jhones_sedex BOOLEAN")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erro ao verificar/adicionar colunas: {e}")
        _log_error(f"Erro SQLite em _add_db_columns_if_not_exists: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")


def criar_tabelas():
    conn, cursor = conectar_db()
    try:
        cursor.execute('CREATE TABLE IF NOT EXISTS killers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)')
        cursor.execute('CREATE TABLE IF NOT EXISTS maps (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)')
        cursor.execute('CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT, match_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                killer_id INTEGER, map_id INTEGER, item_used_id INTEGER, item_gained_id INTEGER,
                item_lost_id INTEGER, escaped BOOLEAN, survivors_escaped INTEGER, notes TEXT,
                game_mode TEXT, jhones_sedex BOOLEAN,
                FOREIGN KEY (killer_id) REFERENCES killers(id), FOREIGN KEY (map_id) REFERENCES maps(id),
                FOREIGN KEY (item_used_id) REFERENCES items(id), FOREIGN KEY (item_gained_id) REFERENCES items(id),
                FOREIGN KEY (item_lost_id) REFERENCES items(id)
            )
        ''')
        cursor.execute('CREATE TABLE IF NOT EXISTS teammates (id INTEGER PRIMARY KEY AUTOINCREMENT, nickname TEXT UNIQUE NOT NULL)')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_teammates (
                match_id INTEGER NOT NULL, teammate_id INTEGER NOT NULL,
                FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
                FOREIGN KEY(teammate_id) REFERENCES teammates(id) ON DELETE CASCADE,
                PRIMARY KEY (match_id, teammate_id)
            )
        ''')
        conn.commit()
        _add_db_columns_if_not_exists(conn, cursor)
        popular_dados_iniciais(conn, cursor)
    except sqlite3.Error as e: 
        print(f"Erro ao criar tabelas: {e}")
        _log_error(f"Erro SQLite em criar_tabelas: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        messagebox.showerror("Erro Crítico de Banco de Dados", f"Não foi possível inicializar o banco de dados: {e}\nA aplicação não pode continuar.")
        sys.exit(1) 
    finally: 
        if conn:
            conn.close()

def popular_dados_iniciais(conn, cursor):
    initial_data = { "killers": ["O Caçador", "O Espectro", "O Caipira", "A Enfermeira", "O Vulto", "A Bruxa", "O Médico", "A Caçadora", "O Canibal", "O Pesadelo", "A Porca", "O Palhaço", "O Espírito", "A Legião", "A Praga", "O Ghostface", "O Demogorgon", "O Oni", "O Mercenário", "O Carrasco", "O Flagelo", "Os Gêmeos", "O Trapaceiro", "O Nêmesis", "O Cenobita", "A Artista", "A Onryō", "A Draga", "O Vilão", "O Cavaleiro", "A Negociante de Crânios", "A Singularidade", "O Xenomorfo", "O Cara Legal", "O Desconhecido", "O Lich", "O Senhor das Trevas", "A Mestra da Matilha", "O Ghoul", "O Animatrônico"], "maps": ["Propriedade MacMillan – Torre de Carvão", "Propriedade MacMillan – Fábrica da Miséria", "Propriedade MacMillan – Abrigo Florestal", "Propriedade MacMillan – Fosso do Sufocamento", "Propriedade MacMillan – Armazém Rangente", "Destroços de Autohaven – Sepultura de Azarov", "Destroços de Autohaven – Paraíso do Combustível", "Destroços de Autohaven – Loja Desgraçada", "Destroços de Autohaven – Abrigo Sangrento", "Destroços de Autohaven – Quintal do Ferro Velho", "Fazenda Coldwind – Campos Pútridos", "Fazenda Coldwind – Casa dos Thompson", "Fazenda Coldwind – Estábulo Fraturado", "Fazenda Coldwind – Abatedouro Asqueroso", "Fazenda Coldwind – Córrego Atormentador", "Hospício Crotus Prenn – Capela do Padre Campbell", "Hospício Crotus Prenn – Enfermaria Conturbada", "Haddonfield – Travessa Lampkin", "Pântano do Remanso – A Rosa Lívida", "Pântano do Remanso – Despensa Cruel", "Instituto Memorial Léry – Centro de Tratamento", "Floresta Vermelha – Refúgio da Caçadora", "Floresta Vermelha – O Templo da Purgação", "Springwood – Escola Primária de Badham I", "Springwood – Escola Primária de Badham II", "Springwood – Escola Primária de Badham III", "Springwood – Escola Primária de Badham IV", "Springwood – Escola Primária de Badham V", "O Jogo – Fábrica de Embalagens de Carnes Gideon", "Propriedade dos Yamaoka – Residência da Família", "Propriedade dos Yamaoka – Santuário da Ira", "Ormond – Resort do Monte Ormond", "Ormond – Mina do Lago de Ormond", "Túmulo de Glenvale – Saloon do Cachorro Morto", "Raccoon City – Delegacia (Ala Leste)", "Raccoon City – Delegacia (Ala Oeste)", "Cemitério Renegado – Ninho dos Corvos", "Ilha sem Vida – Jardim da Alegria", "Ilha sem Vida – Praça de Greenville", "Ilha sem Vida – Freddy Fazbear's Pizza", "Floresta de Dvarka – Pouso do Lago Toba", "Floresta de Dvarka – Destroços da Nostromo", "Borgo Dizimado – Praça Arrasada", "Borgo Dizimado – Ruínas Esquecidas"], "items": ["Nenhum", "Caixa de Ferramentas Gasta", "Caixa de Ferramentas Comum", "Caixa de Ferramentas do Mecânico", "Caixa de Ferramentas Grande", "Caixa de Ferramentas de Alex", "Caixa de Ferramentas da Engenheira", "Kit Médico de Acampamento", "Kit de Primeiros Socorros", "Kit Médico de Emergência", "Kit Médico de Patrulheiro", "Lanterna Comum", "Lanterna Esportiva", "Lanterna Utilitária", "Chave Quebrada", "Chave Gasta", "Chave Esqueleto", "Mapa Comum", "Mapa Arco-Íris", "Fogos de Artifício (Evento)", "Lanterna Chinesa (Evento)", "Lanterna de Ano Novo Lunar (Evento)"] }
    try:
        for table_name, data_list in initial_data.items():
            for item_name in data_list: cursor.execute(f"INSERT OR IGNORE INTO {table_name} (name) VALUES (?)", (item_name,))
        conn.commit()
    except sqlite3.Error as e: 
        print(f"Erro ao popular dados iniciais: {e}")
        _log_error(f"Erro SQLite em popular_dados_iniciais: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")

def buscar_items_genericos(table_name, order_by_name=True):
    conn, cursor = conectar_db()
    try:
        order_clause = "ORDER BY name COLLATE NOCASE" if order_by_name else ""
        cursor.execute(f"SELECT id, name FROM {table_name} {order_clause}")
        return cursor.fetchall()
    except sqlite3.Error as e: 
        print(f"Erro ao buscar {table_name}: {e}")
        _log_error(f"Erro SQLite em buscar_items_genericos para '{table_name}': {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        return []
    finally: 
        if conn:
            conn.close()

def get_or_create_teammate_id(nickname, conn_cursor_tuple):
    conn, cursor = conn_cursor_tuple
    if not nickname or not nickname.strip(): return None
    try:
        cursor.execute("SELECT id FROM teammates WHERE LOWER(nickname) = LOWER(?)", (nickname.strip(),))
        result = cursor.fetchone()
        if result: return result[0]
        else:
            cursor.execute("INSERT INTO teammates (nickname) VALUES (?)", (nickname.strip(),))
            return cursor.lastrowid
    except sqlite3.Error as e: 
        print(f"Erro ao buscar/criar teammate {nickname}: {e}")
        _log_error(f"Erro SQLite em get_or_create_teammate_id para '{nickname}': {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        return None

def registrar_partida(killer_id, map_id, item_used_id, item_gained_id, item_lost_id,
                        escaped, survivors_escaped, notes, game_mode,
                        teammates_nicks=None, jhones_sedex=None, match_date_str=None):
    conn, cursor = conectar_db()
    match_date_to_insert = match_date_str if match_date_str else datetime.datetime.now().isoformat()
    match_id = None
    try:
        p_killer_id = int(killer_id) if killer_id is not None else None
        p_map_id = int(map_id) if map_id is not None else None
        p_item_used_id = int(item_used_id) if item_used_id is not None else None
        p_item_gained_id = int(item_gained_id) if item_gained_id is not None else None
        p_item_lost_id = int(item_lost_id) if item_lost_id is not None else None
        p_escaped = bool(escaped) if escaped is not None else None
        p_survivors_escaped = int(survivors_escaped) if survivors_escaped is not None else None
        p_notes = str(notes) if notes is not None else ""
        p_game_mode = str(game_mode) if game_mode is not None else None
        p_jhones_sedex = bool(jhones_sedex) if jhones_sedex is not None else None
        p_match_date = str(match_date_to_insert)

        params_sql = (
            p_killer_id, p_map_id, p_item_used_id, p_item_gained_id, p_item_lost_id,
            p_escaped, p_survivors_escaped, p_notes, p_game_mode,
            p_jhones_sedex, p_match_date
        )
        cursor.execute('''
            INSERT INTO matches (killer_id, map_id, item_used_id, item_gained_id, item_lost_id,
                                    escaped, survivors_escaped, notes, game_mode, jhones_sedex, match_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', params_sql)
        match_id = cursor.lastrowid
        if match_id and teammates_nicks:
            for nick in teammates_nicks:
                if nick and nick.strip():
                    teammate_id = get_or_create_teammate_id(nick, (conn, cursor))
                    if teammate_id:
                        cursor.execute("INSERT OR IGNORE INTO match_teammates (match_id, teammate_id) VALUES (?, ?)", (match_id, teammate_id))
        conn.commit()
        if not match_date_str: messagebox.showinfo("Sucesso", "Partida registrada com sucesso!")
        return True
    except TypeError as te:
        if conn: conn.rollback()
        print(f"TypeError em registrar_partida: {te}")
        _log_error(f"TypeError em registrar_partida: {te}\n{traceback.format_exc()}", "ganchometro_save_error.txt")
        if not match_date_str: messagebox.showerror("Erro de Tipo ao Salvar", f"Erro de tipo ao salvar: {te}\nVerifique o console e o log 'ganchometro_save_error.txt'.")
        return False
    except sqlite3.Error as e:
        if conn: conn.rollback()
        print(f"Erro ao registrar partida no DB: {e}")
        _log_error(f"Erro SQLite em registrar_partida: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        if not match_date_str: messagebox.showerror("Erro de BD", f"Erro ao registrar partida: {e}\nVerifique o console e o log 'ganchometro_sqlite_errors.txt'.")
        return False
    finally: 
        if conn:
            conn.close()

def buscar_historico_partidas():
    conn, cursor = conectar_db()
    try:
        cursor.execute('''
            SELECT m.id, strftime('%d/%m/%Y %H:%M', m.match_date) AS data_partida, k.name AS assassino, mp.name AS mapa,
                   iu.name AS item_usado, ig.name AS item_ganho, il.name AS item_perdido,
                   CASE m.escaped WHEN 1 THEN 'Sim' ELSE 'Não' END AS sobreviveu,
                   m.survivors_escaped AS qts_escaparam, IFNULL(m.game_mode, 'N/D') AS jogando_como,
                   (SELECT GROUP_CONCAT(t.nickname, ', ') FROM teammates t JOIN match_teammates mt ON t.id = mt.teammate_id WHERE mt.match_id = m.id) AS companheiros,
                   CASE m.jhones_sedex WHEN 1 THEN 'Sim' WHEN 0 THEN 'Não' ELSE 'N/D' END AS jhones_sedex_info,
                   m.notes AS notas_adicionais
            FROM matches m LEFT JOIN killers k ON m.killer_id = k.id LEFT JOIN maps mp ON m.map_id = mp.id
            LEFT JOIN items iu ON m.item_used_id = iu.id LEFT JOIN items ig ON m.item_gained_id = ig.id
            LEFT JOIN items il ON m.item_lost_id = il.id ORDER BY m.match_date DESC
        ''')
        return [tuple(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: 
        print(f"Erro ao buscar histórico: {e}")
        _log_error(f"Erro SQLite em buscar_historico_partidas: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
        return []
    finally: 
        if conn:
            conn.close()

def calcular_estatisticas_gerais():
    conn, cursor = conectar_db()
    stats = { "total_partidas": 0, "escapes_totais": 0, "taxa_sobrevivencia_geral": 0.0, 
              "partidas_por_killer": {}, "sobrevivencia_por_killer": {}, 
              "itens_levados_count": {}, "itens_perdidos_count": {}, 
              "jogos_por_mapa": {}, "partidas_por_modo": {}, "sobrevivencia_por_modo": {}, 
              "sobrevivencia_com_teammate": {}, 
              "jhones_sedex_sim": 0, "jhones_sedex_nao": 0, "partidas_com_jhones_respondido":0,
              "killer_mais_enfrentado": {"nome": "N/A", "contagem": 0}, 
              "mapa_mais_jogado": {"nome": "N/A", "contagem": 0} 
            }
    try:
        cursor.execute("SELECT COUNT(*), SUM(escaped) FROM matches"); res = cursor.fetchone()
        if res: stats["total_partidas"], stats["escapes_totais"] = (res[0] or 0), (res[1] or 0)
        if stats["total_partidas"] > 0: stats["taxa_sobrevivencia_geral"] = (stats["escapes_totais"] / stats["total_partidas"]) * 100
        
        cursor.execute('SELECT k.name, COUNT(m.id) AS count_killer, SUM(m.escaped) FROM matches m JOIN killers k ON m.killer_id = k.id GROUP BY k.name ORDER BY count_killer DESC')
        killer_details = cursor.fetchall()
        if killer_details:
            stats["killer_mais_enfrentado"]["nome"] = killer_details[0]["name"]
            stats["killer_mais_enfrentado"]["contagem"] = killer_details[0]["count_killer"]
            for r_killer in killer_details:
                killer_name = r_killer["name"]
                count_killer = r_killer["count_killer"] or 0
                escapes_killer = r_killer["SUM(m.escaped)"] or 0
                stats["partidas_por_killer"][killer_name] = count_killer
                stats["sobrevivencia_por_killer"][killer_name] = (escapes_killer / count_killer) * 100 if count_killer > 0 else 0.0
        
        cursor.execute("SELECT i.name, COUNT(m.id) FROM matches m JOIN items i ON m.item_used_id = i.id WHERE i.name != 'Nenhum' GROUP BY i.name ORDER BY COUNT(m.id) DESC")
        for r in cursor.fetchall(): stats["itens_levados_count"][r[0]] = r[1] 
        
        cursor.execute("SELECT i.name, COUNT(m.id) FROM matches m JOIN items i ON m.item_lost_id = i.id WHERE i.name != 'Nenhum' GROUP BY i.name ORDER BY COUNT(m.id) DESC")
        for r in cursor.fetchall(): stats["itens_perdidos_count"][r[0]] = r[1] 
        
        cursor.execute("SELECT mp.name, COUNT(m.id) AS count_map FROM matches m JOIN maps mp ON m.map_id = mp.id GROUP BY mp.name ORDER BY COUNT(m.id) DESC")
        map_details = cursor.fetchall()
        if map_details:
            stats["mapa_mais_jogado"]["nome"] = map_details[0]["name"]
            stats["mapa_mais_jogado"]["contagem"] = map_details[0]["count_map"]
            for r_map in map_details: stats["jogos_por_mapa"][r_map["name"]] = r_map["count_map"]

        cursor.execute("SELECT game_mode, COUNT(id), SUM(escaped) FROM matches WHERE game_mode IS NOT NULL AND game_mode != '' GROUP BY game_mode")
        for r in cursor.fetchall(): stats["partidas_por_modo"][r[0]], stats["sobrevivencia_por_modo"][r[0]] = r[1] or 0, (((r[2] or 0) / (r[1] or 1)) * 100) 
        
        cursor.execute('SELECT t.nickname, COUNT(mt.match_id), SUM(m.escaped) FROM teammates t JOIN match_teammates mt ON t.id = mt.teammate_id JOIN matches m ON mt.match_id = m.id GROUP BY t.nickname')
        for nick, count, esc in cursor.fetchall(): stats["sobrevivencia_com_teammate"][nick] = {"partidas": count, "escapes": esc or 0, "taxa_escape": ((esc or 0) / count) * 100 if count > 0 else 0.0}
        
        cursor.execute("SELECT SUM(CASE WHEN jhones_sedex = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN jhones_sedex = 0 THEN 1 ELSE 0 END), COUNT(CASE WHEN jhones_sedex IS NOT NULL THEN 1 END) FROM matches")
        res_jhones = cursor.fetchone()
        if res_jhones: stats["jhones_sedex_sim"], stats["jhones_sedex_nao"], stats["partidas_com_jhones_respondido"] = (res_jhones[0] or 0), (res_jhones[1] or 0), (res_jhones[2] or 0)
    except sqlite3.Error as e: 
        print(f"Erro ao calcular estatísticas: {e}")
        _log_error(f"Erro SQLite em calcular_estatisticas_gerais: {e}\n{traceback.format_exc()}", "ganchometro_sqlite_errors.txt")
    finally: 
        if conn:
            conn.close()
    return stats

def _formatar_contagem_vezes(contagem):
    return "vez" if contagem == 1 else "vezes"

class DBDTrackerApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1100x800")
        self.resizable(False, False) 
        ctk.set_appearance_mode("Dark")
        self.configure(fg_color=COLOR_BACKGROUND)

        self._image_references = []
        self._portraits_cache = {} 
        self._killer_buttons_ui = {} 
        self._selected_killer_button_ui = None 

        if hasattr(sys, '_MEIPASS'):
            self.base_dir = sys._MEIPASS
        else:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        initialize_database()

        os.makedirs(os.path.join(self.base_dir, IMAGE_ASSETS_PATH), exist_ok=True) 
        os.makedirs(os.path.join(self.base_dir, KILLER_PORTRAITS_PATH), exist_ok=True) 
        os.makedirs(os.path.join(self.base_dir, ITEM_ICONS_PATH), exist_ok=True) 
        
        self._current_match_data = {}
        self._current_step_frame = None 
        self._registration_step_history = [] 
        
        self.main_header_frame = ctk.CTkFrame(self, fg_color=COLOR_BACKGROUND, height=LOGO_TARGET_HEIGHT + 10) 
        self.main_header_frame.pack(side="top", fill="x", padx=10, pady=(10,0))
        self.main_header_frame.grid_columnconfigure(1, weight=1) 

        self.logo_image_ctk = self._load_image(LOGO_FILENAME, LOGO_TARGET_HEIGHT) 
        if self.logo_image_ctk:
            self.logo_label = ctk.CTkLabel(self.main_header_frame, image=self.logo_image_ctk, text="")
            self.logo_label.grid(row=0, column=0, padx=(10,0), pady=5, sticky="w")
        else:
            self.logo_label = ctk.CTkLabel(self.main_header_frame, text="[LOGO]", width=int(LOGO_TARGET_HEIGHT * (3047/1027)), height=LOGO_TARGET_HEIGHT, fg_color="gray30") 
            self.logo_label.grid(row=0, column=0, padx=(10,0), pady=5, sticky="w")
        
        self.settings_button_var = ctk.StringVar()
        self.settings_options = ["Gerenciar Dados", "Sobre"]
        self.settings_button = ctk.CTkOptionMenu(self.main_header_frame,
                                                 variable=self.settings_button_var,
                                                 values=self.settings_options,
                                                 command=self._on_settings_menu_select,
                                                 width=50,
                                                 fg_color=COLOR_BUTTON_SECONDARY,
                                                 button_color=COLOR_BUTTON_SECONDARY,
                                                 button_hover_color=COLOR_BUTTON_HOVER_SECONDARY,
                                                 dropdown_fg_color=COLOR_FRAME_BG,
                                                 dropdown_hover_color=COLOR_BUTTON_HOVER_SECONDARY,
                                                 text_color=COLOR_TEXT,
                                                 font=ctk.CTkFont(size=18))
        self.settings_button.grid(row=0, column=2, padx=(0,10), pady=5, sticky="e")
        self.settings_button_var.set("⚙️")

        self.close_settings_button = ctk.CTkButton(self.main_header_frame,
                                                   text="X",
                                                   command=self._close_settings_view,
                                                   width=40,
                                                   height=40,
                                                   font=ctk.CTkFont(size=16, weight="bold"),
                                                   fg_color=COLOR_PRIMARY_RED,
                                                   hover_color=COLOR_SECONDARY_RED)

        self.tab_view_container = ctk.CTkFrame(self, fg_color=COLOR_BACKGROUND) 
        self.tab_view_container.pack(side="top", fill="x", padx=10, pady=(5,5))
        
        self.tab_view = ctk.CTkTabview(self.tab_view_container, 
                                       anchor="n",
                                       height=45,
                                       fg_color="transparent", 
                                       segmented_button_fg_color=COLOR_BUTTON_SECONDARY,
                                       segmented_button_selected_color=COLOR_PRIMARY_RED,
                                       segmented_button_selected_hover_color=COLOR_SECONDARY_RED,
                                       segmented_button_unselected_hover_color=COLOR_BUTTON_HOVER_SECONDARY,
                                       text_color=COLOR_TEXT,
                                       text_color_disabled=COLOR_TEXT_SUBTLE,
                                       border_width=0)
        self.tab_view.pack(fill="x", expand=True)

        self.content_container = ctk.CTkFrame(self, fg_color=COLOR_BACKGROUND)
        self.content_container.pack(side="top", fill="both", expand=True, padx=10, pady=(0,10))
        
        self.page_frames = {}
        self.page_frames["Registrar Partida"] = ctk.CTkFrame(self.content_container, fg_color=COLOR_FRAME_BG)
        self.criar_aba_registrar_steps_content(self.page_frames["Registrar Partida"])

        self.page_frames["Histórico"] = ctk.CTkFrame(self.content_container, fg_color=COLOR_FRAME_BG)
        self.criar_aba_historico_content(self.page_frames["Histórico"])

        self.page_frames["Estatísticas"] = ctk.CTkFrame(self.content_container, fg_color=COLOR_FRAME_BG)
        self.criar_aba_estatisticas_content(self.page_frames["Estatísticas"]) 

        self.page_frames["Gerenciar Dados"] = ctk.CTkFrame(self.content_container, fg_color=COLOR_FRAME_BG)
        self.criar_aba_gerenciar_dados_content(self.page_frames["Gerenciar Dados"])
        
        self.page_frames["Sobre"] = ctk.CTkFrame(self.content_container, fg_color=COLOR_FRAME_BG)
        self.criar_aba_sobre_content(self.page_frames["Sobre"])
        
        self.tab_view.add("Registrar Partida")
        self.tab_view.add("Histórico")
        self.tab_view.add("Estatísticas")
        
        self.teammate_nicks_comboboxes = []
        self.jhones_sedex_value_step = None 

        self.tab_view.configure(command=self._on_tab_change_v074) 
        
        self._current_page_name = None
        self._is_settings_view_active = False
        self.show_page("Registrar Partida")
    
    def _on_tab_change_v074(self): 
        selected_tab_name = self.tab_view.get()
        self.show_page(selected_tab_name)

    def _on_settings_menu_select(self, selection):
        if selection in self.settings_options:
            self._is_settings_view_active = True
            self.tab_view_container.pack_forget() 
            self.settings_button.grid_forget()
            self.close_settings_button.grid(row=0, column=2, padx=(0,10), pady=5, sticky="e")
            self.show_page(selection)
        self.settings_button_var.set("⚙️")

    def _close_settings_view(self):
        self._is_settings_view_active = False
        if self._current_page_name in self.page_frames and self.page_frames[self._current_page_name]:
             if self.page_frames[self._current_page_name].winfo_ismapped():
                self.page_frames[self._current_page_name].pack_forget()

        self.close_settings_button.grid_forget()
        self.settings_button.grid(row=0, column=2, padx=(0,10), pady=5, sticky="e")
        self.tab_view_container.pack(side="top", fill="x", padx=10, pady=(5,5), before=self.content_container) 
        
        last_main_tab = self.tab_view.get() if self.tab_view.get() else "Registrar Partida"
        self.show_page(last_main_tab)

    def show_page(self, page_name):
        if self._current_page_name == page_name and not self._is_settings_view_active and page_name in ["Registrar Partida", "Histórico", "Estatísticas"]:
            if page_name == "Histórico":
                self.carregar_historico()
            elif page_name == "Estatísticas":
                self.carregar_estatisticas_view() 
            return

        if self._current_page_name == page_name and self._is_settings_view_active and page_name in ["Gerenciar Dados", "Sobre"]:
            return

        if self._current_page_name and self._current_page_name in self.page_frames:
            if self.page_frames[self._current_page_name] and self.page_frames[self._current_page_name].winfo_exists():
                self.page_frames[self._current_page_name].pack_forget()

        frame_to_show = self.page_frames.get(page_name)
        if frame_to_show:
            frame_to_show.pack(in_=self.content_container, fill="both", expand=True)
            self._current_page_name = page_name

            if page_name == "Registrar Partida":
                if hasattr(self, '_step_killer_frame') and self._current_step_frame != self._step_killer_frame: 
                    self.reset_match_registration()
            elif page_name == "Histórico":
                self.carregar_historico()
            elif page_name == "Estatísticas":
                self.carregar_estatisticas_view() 
            
            if page_name in ["Registrar Partida", "Histórico", "Estatísticas"] and not self._is_settings_view_active:
                if self.tab_view.get() != page_name:
                    self.tab_view.set(page_name)
        else:
            print(f"Erro: Frame para a página '{page_name}' não encontrado.")
            _log_error(f"Tentativa de mostrar página não existente: {page_name}", "ganchometro_ui_errors.txt")


    def _load_image(self, filename_or_basename, target_size_or_height, is_killer_portrait=False, base_folder_override=None):
        if isinstance(target_size_or_height, int): 
            size_key = target_size_or_height
        else: 
            size_key = tuple(target_size_or_height)
            
        cache_key = (filename_or_basename, size_key, is_killer_portrait, base_folder_override)

        if cache_key in self._portraits_cache and self._portraits_cache[cache_key] is not None:
             return self._portraits_cache[cache_key]

        actual_base_folder = base_folder_override if base_folder_override else (KILLER_PORTRAITS_PATH if is_killer_portrait else IMAGE_ASSETS_PATH)
        full_base_path = os.path.join(self.base_dir, actual_base_folder)
        
        name_part, ext_part = os.path.splitext(filename_or_basename)
        potential_files_to_check = []
        cleaned_name_part = name_part.replace(":", "") 

        if is_killer_portrait or not ext_part: 
            potential_files_to_check.extend([cleaned_name_part + ".png", cleaned_name_part + ".jpg", cleaned_name_part + ".jpeg"])
        else: 
            potential_files_to_check.append(filename_or_basename)
        
        final_img_path = None
        for fname_to_check in potential_files_to_check:
            p_path = os.path.join(full_base_path, fname_to_check)
            if os.path.exists(p_path): final_img_path = p_path; break

        if final_img_path:
            try:
                pil_image = Image.open(final_img_path)
                
                if filename_or_basename == LOGO_FILENAME and isinstance(target_size_or_height, int):
                    target_height_logo = target_size_or_height 
                    original_width, original_height = pil_image.size
                    if original_height == 0: return None 
                    aspect_ratio = original_width / original_height
                    new_width = int(target_height_logo * aspect_ratio)
                    resized_image = pil_image.resize((new_width, target_height_logo), Image.Resampling.LANCZOS)
                elif isinstance(target_size_or_height, tuple): 
                    resized_image = pil_image.resize(target_size_or_height, Image.Resampling.LANCZOS)
                else: 
                    resized_image = pil_image.resize(target_size_or_height, Image.Resampling.LANCZOS)


                ctk_image = ImageTk.PhotoImage(resized_image)
                self._portraits_cache[cache_key] = ctk_image
                self._image_references.append(ctk_image)
                return ctk_image
            except FileNotFoundError:
                print(f"Arquivo de imagem não encontrado: '{final_img_path}'")
                _log_error(f"Arquivo de imagem não encontrado em _load_image: '{final_img_path}'", "ganchometro_image_errors.txt")
            except UnidentifiedImageError: 
                 print(f"Não foi possível identificar o arquivo de imagem (corrompido ou formato inválido): '{final_img_path}'")
                 _log_error(f"PIL.UnidentifiedImageError em _load_image para '{final_img_path}'", "ganchometro_image_errors.txt")
            except Exception as e: 
                print(f"Erro PIL/IO ao abrir/processar imagem '{final_img_path}': {e}")
                _log_error(f"Erro PIL/IO em _load_image para '{final_img_path}': {e}\n{traceback.format_exc()}", "ganchometro_image_errors.txt")
        else:
            print(f"Caminho final da imagem não encontrado para: {filename_or_basename} em {full_base_path}")
            _log_error(f"Caminho final da imagem não encontrado para: {filename_or_basename} em {full_base_path}", "ganchometro_image_errors.txt")

        self._portraits_cache[cache_key] = None
        return None

    def criar_aba_registrar_steps_content(self, tab_registrar): 
        self.main_registrar_frame = ctk.CTkFrame(tab_registrar, fg_color=COLOR_FRAME_BG)
        self.main_registrar_frame.pack(fill="both", expand=True) 
        
        self.cancel_button_header_steps = ctk.CTkButton(self.main_registrar_frame, text="Voltar ao Início do Registro",
                                                 command=self.reset_match_registration,
                                                 fg_color=COLOR_BUTTON_SECONDARY,
                                                 hover_color=COLOR_BUTTON_HOVER_SECONDARY)

        self.steps_container_frame = ctk.CTkFrame(self.main_registrar_frame, fg_color="transparent")
        self.steps_container_frame.pack(fill="both", expand=True, padx=10, pady=(0,5)) 
        self._step_killer_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_map_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_item_used_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_item_gained_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_item_lost_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_playing_mode_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_escaped_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_survivors_escaped_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        self._step_notes_save_frame = ctk.CTkFrame(self.steps_container_frame, fg_color="transparent")
        
        self.reset_match_registration() 


    def _select_killer_ui_feedback(self, killer_card_frame_to_select):
        if self._selected_killer_button_ui:
            try: self._selected_killer_button_ui.configure(border_width=1, border_color="gray50")
            except Exception: pass 
        if killer_card_frame_to_select:
            killer_card_frame_to_select.configure(border_width=2, border_color=COLOR_BORDER_SELECTED)
        self._selected_killer_button_ui = killer_card_frame_to_select

    def _show_step(self, step_frame_to_show, came_from_back_button=False):
        if self._current_step_frame: self._current_step_frame.pack_forget()
        self._current_step_frame = step_frame_to_show
        self._current_step_frame.pack(fill="both", expand=True)

        if hasattr(self, 'cancel_button_header_steps'): 
            if step_frame_to_show == self._step_killer_frame:
                if self.cancel_button_header_steps.winfo_ismapped():
                    self.cancel_button_header_steps.pack_forget()
            else:
                if not self.cancel_button_header_steps.winfo_ismapped():
                    if hasattr(self.main_registrar_frame, 'winfo_exists') and self.main_registrar_frame.winfo_exists():
                         self.cancel_button_header_steps.pack(in_=self.main_registrar_frame, side="top", pady=(5,10), padx=10, anchor="e", before=self.steps_container_frame)


        if not came_from_back_button:
            if not self._registration_step_history or self._registration_step_history[-1] != step_frame_to_show:
                self._registration_step_history.append(step_frame_to_show)
    
    def _go_back_step(self):
        if len(self._registration_step_history) > 1:
            self._registration_step_history.pop() 
            previous_step_frame = self._registration_step_history[-1]
            if previous_step_frame == self._step_killer_frame:
                keys_to_clear = list(self._current_match_data.keys()) 
                for k in keys_to_clear: del self._current_match_data[k]
                self._select_killer_ui_feedback(None) 
                self._build_step_killer_selection() 
            elif previous_step_frame == self._step_map_frame:
                for k in ['item_used_id', 'item_gained_id', 'item_lost_id', 'game_mode', 'teammates', 'escaped', 'survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._create_selection_grid_step(self._step_map_frame, "Escolha o Mapa", buscar_items_genericos("maps"), self._action_select_map_item_generic, "map", back_command=lambda: self._show_step(self._step_killer_frame))
            elif previous_step_frame == self._step_item_used_frame:
                for k in ['item_gained_id', 'item_lost_id', 'game_mode', 'teammates', 'escaped', 'survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._create_selection_grid_step(self._step_item_used_frame, "Item Usado", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_used", back_command=lambda: self._show_step(self._step_map_frame))
            elif previous_step_frame == self._step_item_gained_frame:
                for k in ['item_lost_id', 'game_mode', 'teammates', 'escaped', 'survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._create_selection_grid_step(self._step_item_gained_frame, "Item Ganho", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_gained", back_command=lambda: self._show_step(self._step_item_used_frame))
            elif previous_step_frame == self._step_item_lost_frame:
                for k in ['game_mode', 'teammates', 'escaped', 'survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._create_selection_grid_step(self._step_item_lost_frame, "Item Perdido", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_lost", back_command=lambda: self._show_step(self._step_item_gained_frame))
            elif previous_step_frame == self._step_playing_mode_frame:
                for k in ['escaped', 'survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._build_step_playing_mode()
            elif previous_step_frame == self._step_escaped_frame:
                for k in ['survivors_escaped', 'notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                self._build_step_escaped()
            elif previous_step_frame == self._step_survivors_escaped_frame:
                 for k in ['notes', 'jhones_sedex']: self._current_match_data.pop(k, None)
                 self._build_step_survivors_escaped()
            self._show_step(previous_step_frame, came_from_back_button=True)

    def reset_match_registration(self):
        self._current_match_data = {}
        self._select_killer_ui_feedback(None)
        self._registration_step_history = []
        if hasattr(self, 'teammate_nicks_comboboxes'):
            for combo in self.teammate_nicks_comboboxes: combo.set("")
        if hasattr(self, 'notes_text_step') and self.notes_text_step.winfo_exists():
             self.notes_text_step.delete("1.0", "end")
        self.jhones_sedex_value_step = None 
        if hasattr(self, 'jhones_question_frame_step') and self.jhones_question_frame_step.winfo_exists() and self.jhones_question_frame_step.winfo_ismapped():
            self.jhones_question_frame_step.pack_forget()
        
        if hasattr(self, '_step_killer_frame') and self._step_killer_frame:
            self._build_step_killer_selection()
            self._show_step(self._step_killer_frame)


    def _build_navigation_buttons(self, parent_frame, back_command=None, next_command=None, next_text="Próximo"):
        for widget in parent_frame.winfo_children():
            if isinstance(widget, ctk.CTkFrame) and \
               any(isinstance(grandchild, ctk.CTkButton) and \
                   ("< Voltar" in grandchild.cget("text") or ">" in grandchild.cget("text")) \
                   for grandchild in widget.winfo_children()):
                widget.destroy()

        nav_frame = ctk.CTkFrame(parent_frame, fg_color="transparent")
        nav_frame.pack(pady=(15,5), fill="x", side="bottom", anchor="s")
        if back_command:
            ctk.CTkButton(nav_frame, text="< Voltar", command=back_command, width=120, fg_color=COLOR_BUTTON_SECONDARY, hover_color=COLOR_BUTTON_HOVER_SECONDARY).pack(side="left", padx=10)
        else: 
            ctk.CTkLabel(nav_frame, text="", width=120, fg_color="transparent").pack(side="left", padx=10) 
        if next_command:
            ctk.CTkButton(nav_frame, text=next_text + " >", command=next_command, width=120, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY).pack(side="right", padx=10)

    def _build_step_killer_selection(self):
        parent_frame = self._step_killer_frame
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text="Escolha o Assassino", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=(5,5))
        
        killer_scroll_frame = ctk.CTkScrollableFrame(parent_frame, orientation="vertical", fg_color=COLOR_BACKGROUND) 
        killer_scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        if hasattr(self, '_killer_buttons_ui'): self._killer_buttons_ui.clear()
        else: self._killer_buttons_ui = {}
        
        all_killers = buscar_items_genericos("killers", order_by_name=True)
        cols = 10 
        row_idx, col_idx = 0, 0
        
        for killer_id, killer_name in all_killers:
            killer_card_frame = ctk.CTkFrame(killer_scroll_frame, border_width=1, border_color="gray50", fg_color=COLOR_FRAME_BG)
            killer_card_frame.grid_propagate(False)
            killer_card_frame.configure(width=KILLER_CARD_WIDTH, height=KILLER_CARD_HEIGHT) 
            killer_card_frame.grid(row=row_idx, column=col_idx, padx=2, pady=2, sticky="nsew") 

            image_name_base = killer_name.replace(" ", "_").replace(":", "")
            ctk_image = self._load_image(image_name_base, PORTRAIT_SIZE_BUTTON_KILLER, is_killer_portrait=True)
            img_display_text = "" if ctk_image else "S/Img"
            img_display = ctk.CTkLabel(killer_card_frame, text=img_display_text, image=ctk_image, width=PORTRAIT_SIZE_BUTTON_KILLER[0], height=PORTRAIT_SIZE_BUTTON_KILLER[1], fg_color="gray25" if not ctk_image else "transparent")
            img_display.pack(pady=(3,1), padx=3)
            
            name_label = ctk.CTkLabel(killer_card_frame, text=killer_name, font=ctk.CTkFont(size=10), text_color=COLOR_TEXT, wraplength=KILLER_CARD_WIDTH - 10) 
            name_label.pack(pady=(0,3), padx=3, fill="x", expand=True)
            
            current_killer_id_temp = self._current_match_data.get('killer_id')
            if current_killer_id_temp == killer_id:
                killer_card_frame.configure(border_width=2, border_color=COLOR_BORDER_SELECTED)
                self._selected_killer_button_ui = killer_card_frame
            
            command = lambda event_obj, k_id=killer_id, k_name=killer_name, k_frame=killer_card_frame: self._action_select_killer(k_id, k_name, k_frame, event_arg=event_obj)
            killer_card_frame.bind("<Button-1>", command); img_display.bind("<Button-1>", command); name_label.bind("<Button-1>", command)
            self._killer_buttons_ui[killer_id] = killer_card_frame
            col_idx +=1
            if col_idx >= cols: col_idx = 0; row_idx += 1
        for i in range(cols): 
            killer_scroll_frame.grid_columnconfigure(i, weight=1)
    
    def _create_selection_grid_step(self, parent_frame, title, items_list, action_callback, item_type_for_callback, back_command):
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text=title, font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=(5,10))
        scroll_frame = ctk.CTkScrollableFrame(parent_frame, fg_color=COLOR_BACKGROUND) 
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
        cols = 3; row_idx, col_idx = 0, 0
        for item_id, item_name in items_list:
            item_card_frame = ctk.CTkFrame(scroll_frame, width=MAP_ITEM_CARD_WIDTH, height=MAP_ITEM_CARD_HEIGHT, fg_color=COLOR_BUTTON_PRIMARY, border_width=1, border_color=COLOR_PRIMARY_RED, corner_radius=6)
            item_card_frame.grid_propagate(False)
            item_card_frame.grid(row=row_idx, column=col_idx, padx=5, pady=5, sticky="ew")
            label = ctk.CTkLabel(item_card_frame, text=item_name, text_color="#FFFFFF", wraplength=MAP_ITEM_CARD_WIDTH - 20, font=ctk.CTkFont(size=12),justify="center", fg_color="transparent")
            label.place(relx=0.5, rely=0.5, anchor="center")
            command = lambda event_obj, i_id=item_id, i_name=item_name, i_type=item_type_for_callback: action_callback(i_id, i_name, i_type, event_arg=event_obj)
            item_card_frame.bind("<Button-1>", command); label.bind("<Button-1>", command)
            item_card_frame.bind("<Enter>", lambda e, frame=item_card_frame: frame.configure(fg_color=COLOR_BUTTON_HOVER_PRIMARY, border_color=COLOR_BUTTON_HOVER_PRIMARY))
            item_card_frame.bind("<Leave>", lambda e, frame=item_card_frame: frame.configure(fg_color=COLOR_BUTTON_PRIMARY, border_color=COLOR_PRIMARY_RED))
            col_idx +=1
            if col_idx >= cols: col_idx = 0; row_idx += 1
        for i in range(cols): scroll_frame.grid_columnconfigure(i, weight=1)
        self._build_navigation_buttons(parent_frame, back_command=back_command)

    def _action_select_killer(self, killer_id, killer_name, button_frame, event_arg=None):
        if not isinstance(killer_id, int):
            print(f"ALERTA CRÍTICO: _action_select_killer - killer_id NÃO é int. Recebido: {killer_id} (Tipo: {type(killer_id)})")
            if event_arg: print(f"  event_arg também foi: {event_arg}")
            return 
        if hasattr(self, 'cancel_button_header_steps') and not self.cancel_button_header_steps.winfo_ismapped():
             self.cancel_button_header_steps.pack(in_=self.main_registrar_frame, side="top", pady=(5,10), padx=10, anchor="e", before=self.steps_container_frame)
        self._current_match_data['killer_id'] = killer_id
        self._select_killer_ui_feedback(button_frame)
        self._create_selection_grid_step(self._step_map_frame, "Escolha o Mapa", buscar_items_genericos("maps"), self._action_select_map_item_generic, "map", back_command=lambda: self._show_step(self._step_killer_frame))
        self._show_step(self._step_map_frame)

    def _action_select_map_item_generic(self, item_id, item_name, item_type, event_arg=None):
        if not isinstance(item_id, int):
            print(f"ALERTA CRÍTICO: _action_select_map_item_generic ({item_type}) - item_id NÃO é int. Recebido: {item_id} (Tipo: {type(item_id)})")
            if event_arg: print(f"  event_arg também foi: {event_arg}")
            return
            
        self._current_match_data[item_type + '_id'] = item_id
        
        conn_temp, cursor_temp = conectar_db()
        nenhum_id_tuple = cursor_temp.execute("SELECT id FROM items WHERE name = 'Nenhum'").fetchone()
        nenhum_id = nenhum_id_tuple[0] if nenhum_id_tuple else -1 
        conn_temp.close()

        if item_type == "map":
            self._create_selection_grid_step(self._step_item_used_frame, "Item Usado", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_used", back_command=lambda: self._show_step(self._step_map_frame))
            self._show_step(self._step_item_used_frame)
        elif item_type == "item_used":
            if item_id == nenhum_id: 
                self._current_match_data['item_lost_id'] = nenhum_id 
                self._create_selection_grid_step(self._step_item_gained_frame, "Item Ganho", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_gained", back_command=lambda: self._show_step(self._step_item_used_frame))
                self._show_step(self._step_item_gained_frame)
            else: 
                self._current_match_data['item_gained_id'] = nenhum_id 
                self._create_selection_grid_step(self._step_item_lost_frame, "Item Perdido", buscar_items_genericos("items"), self._action_select_map_item_generic, "item_lost", back_command=lambda: self._show_step(self._step_item_used_frame))
                self._show_step(self._step_item_lost_frame)
        elif item_type == "item_gained": 
            self._build_step_playing_mode() 
            self._show_step(self._step_playing_mode_frame)
        elif item_type == "item_lost": 
            self._build_step_playing_mode()
            self._show_step(self._step_playing_mode_frame)

    def _build_step_playing_mode(self):
        parent_frame = self._step_playing_mode_frame
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text="Como você estava jogando?", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=10)
        self.game_mode_var_step = ctk.StringVar(value=self._current_match_data.get('game_mode', GAME_MODES[0]))
        game_mode_options = ctk.CTkOptionMenu(parent_frame, variable=self.game_mode_var_step, values=GAME_MODES, fg_color=COLOR_BUTTON_PRIMARY, button_color=COLOR_PRIMARY_RED, button_hover_color=COLOR_SECONDARY_RED, command=self._update_teammate_fields_step_visibility)
        game_mode_options.pack(pady=10)
        self.teammates_frame_step = ctk.CTkFrame(parent_frame, fg_color="transparent")
        self.teammates_frame_step.pack(pady=5)
        self.teammate_nicks_comboboxes.clear()
        
        conn, cursor = conectar_db()
        cursor.execute("SELECT nickname FROM teammates ORDER BY nickname COLLATE NOCASE")
        existing_nicks = [row['nickname'] for row in cursor.fetchall()]
        conn.close()
        current_teammates_data = self._current_match_data.get('teammates', [])
        for i in range(3):
            combo = ctk.CTkComboBox(self.teammates_frame_step, width=250, values=existing_nicks, fg_color=COLOR_BACKGROUND, border_color="gray50", text_color=COLOR_TEXT, button_color=COLOR_PRIMARY_RED, dropdown_fg_color=COLOR_FRAME_BG, dropdown_hover_color=COLOR_BUTTON_HOVER_SECONDARY)
            if i < len(current_teammates_data): combo.set(current_teammates_data[i])
            self.teammate_nicks_comboboxes.append(combo)
        self._update_teammate_fields_step_visibility()
        
        item_usado_id_val = self._current_match_data.get('item_used_id')
        conn_temp, cursor_temp = conectar_db()
        nenhum_id_val = get_id_by_name("items", "Nenhum", (conn_temp, cursor_temp))
        conn_temp.close()
        
        back_target_for_playing_mode = self._step_item_lost_frame
        if item_usado_id_val == nenhum_id_val:
            back_target_for_playing_mode = self._step_item_gained_frame

        self._build_navigation_buttons(parent_frame, back_command=lambda: self._show_step(back_target_for_playing_mode), next_command=self._action_confirm_playing_mode)

    def _update_teammate_fields_step_visibility(self, event=None):
        selected_mode = self.game_mode_var_step.get()
        num_fields_to_show = 0
        if selected_mode == "Dupla": num_fields_to_show = 1
        elif selected_mode == "Trio": num_fields_to_show = 2
        elif selected_mode == "SWF": num_fields_to_show = 3
        for widget in self.teammates_frame_step.winfo_children(): widget.pack_forget()
        for i, combo in enumerate(self.teammate_nicks_comboboxes):
            if i < num_fields_to_show:
                label_widget = ctk.CTkLabel(self.teammates_frame_step, text=f"Nick Companheiro(a) {i+1}:", text_color=COLOR_TEXT)
                label_widget.pack(pady=(5,0), padx=5, anchor="w")
                combo.pack(pady=(0,5), padx=5, fill="x")
            else: combo.set("")

    def _action_confirm_playing_mode(self, event=None):
        self._current_match_data['game_mode'] = self.game_mode_var_step.get()
        teammates_list = []
        if self._current_match_data['game_mode'] != "Solo":
            num_fields_expected = {"Dupla": 1, "Trio": 2, "SWF": 3}.get(self._current_match_data['game_mode'], 0)
            for i in range(num_fields_expected):
                nick = self.teammate_nicks_comboboxes[i].get().strip()
                if nick: teammates_list.append(nick)
        self._current_match_data['teammates'] = teammates_list
        self._build_step_escaped()
        self._show_step(self._step_escaped_frame)

    def _build_step_escaped(self):
        parent_frame = self._step_escaped_frame
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text="Você sobreviveu?", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=20)
        buttons_frame = ctk.CTkFrame(parent_frame, fg_color="transparent")
        buttons_frame.pack(pady=10)
        ctk.CTkButton(buttons_frame, text="Sim", command=lambda event=None: self._action_set_escaped(True), width=120, height=40, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY).pack(side="left", padx=10)
        ctk.CTkButton(buttons_frame, text="Não", command=lambda event=None: self._action_set_escaped(False), width=120, height=40, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY).pack(side="left", padx=10)
        self._build_navigation_buttons(parent_frame, back_command=lambda: self._show_step(self._step_playing_mode_frame))

    def _action_set_escaped(self, escaped_status, event=None):
        self._current_match_data['escaped'] = escaped_status
        self._build_step_survivors_escaped()
        self._show_step(self._step_survivors_escaped_frame)

    def _build_step_survivors_escaped(self):
        parent_frame = self._step_survivors_escaped_frame
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text="Quantas sobreviventes escaparam (total)?", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=20)
        buttons_frame = ctk.CTkFrame(parent_frame, fg_color="transparent")
        buttons_frame.pack(pady=10)
        start_range = 1 if self._current_match_data.get('escaped', False) else 0
        for i in range(start_range, 5):
            ctk.CTkButton(buttons_frame, text=str(i), command=lambda event=None, count=i: self._action_set_survivors_escaped(count), width=50, height=40, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY).pack(side="left", padx=5)
        self._build_navigation_buttons(parent_frame, back_command=lambda: self._show_step(self._step_escaped_frame))
    
    def _action_set_survivors_escaped(self, count, event=None):
        if self._current_match_data.get('escaped') is True and count == 0:
            messagebox.showwarning("Dados Inválidos", "Se você sobreviveu, pelo menos 1 sobrevivente deve ter escapado.") 
            return
        self._current_match_data['survivors_escaped'] = count
        self._build_step_notes_save()
        self._show_step(self._step_notes_save_frame)

    def _check_secret_code(self, event=None):
        notes_content = self.notes_text_step.get("1.0", "end-1c").strip().lower()
        is_secret_frame_built_and_visible = hasattr(self, 'jhones_question_frame_step') and \
                                            self.jhones_question_frame_step.winfo_exists() and \
                                            self.jhones_question_frame_step.winfo_ismapped()
        if notes_content == SECRET_CODE:
            if not is_secret_frame_built_and_visible:
                self._build_jhones_question()
                nav_buttons_frame = None; children = self._step_notes_save_frame.winfo_children()
                for child_idx in range(len(children) -1, -1, -1):
                    child = children[child_idx]
                    if isinstance(child, ctk.CTkFrame): 
                        is_nav_frame = False
                        for grandchild in child.winfo_children():
                            if isinstance(grandchild, ctk.CTkButton) and ("< Voltar" in grandchild.cget("text") or "Salvar Partida" in grandchild.cget("text")):
                                is_nav_frame = True; break
                        if is_nav_frame: nav_buttons_frame = child; break
                if nav_buttons_frame: self.jhones_question_frame_step.pack(pady=10, before=nav_buttons_frame)
                else: self.jhones_question_frame_step.pack(pady=10)
        elif is_secret_frame_built_and_visible:
            self.jhones_question_frame_step.pack_forget()
            self.jhones_sedex_value_step = None
            if 'jhones_sedex' in self._current_match_data: del self._current_match_data['jhones_sedex']

    def _set_jhones_sedex_value(self, value, event=None):
        self.jhones_sedex_value_step = value
        is_sim_selected = value is True; is_nao_selected = value is False
        self.jhones_sedex_btn_sim.configure(fg_color=COLOR_BORDER_SELECTED if is_sim_selected else COLOR_BUTTON_PRIMARY)
        self.jhones_sedex_btn_nao.configure(fg_color=COLOR_BORDER_SELECTED if is_nao_selected else COLOR_BUTTON_PRIMARY)

    def _build_jhones_question(self):
        if not hasattr(self, 'jhones_question_frame_step') or not self.jhones_question_frame_step.winfo_exists() or self.jhones_question_frame_step.master != self._step_notes_save_frame:
            self.jhones_question_frame_step = ctk.CTkFrame(self._step_notes_save_frame, fg_color="transparent")
        else:
            for widget in self.jhones_question_frame_step.winfo_children(): widget.destroy()
        self.jhones_sedex_value_step = self._current_match_data.get('jhones_sedex', None) 
        ctk.CTkLabel(self.jhones_question_frame_step, text="Código secreto ativado!", font=ctk.CTkFont(size=12, weight="bold", slant="italic"), text_color=COLOR_SECONDARY_RED).pack(pady=(0,5), anchor="center")
        question_label_frame = ctk.CTkFrame(self.jhones_question_frame_step, fg_color="transparent"); question_label_frame.pack()
        ctk.CTkLabel(question_label_frame, text="O Jhones deu sedex nesse jogo?", font=ctk.CTkFont(size=14), text_color=COLOR_TEXT).pack(side="left", padx=5)
        buttons_frame = ctk.CTkFrame(self.jhones_question_frame_step, fg_color="transparent"); buttons_frame.pack(pady=5)
        self.jhones_sedex_btn_sim = ctk.CTkButton(buttons_frame, text="Sim", width=70, fg_color=COLOR_BORDER_SELECTED if self.jhones_sedex_value_step is True else COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY, command=lambda event=None: self._set_jhones_sedex_value(True))
        self.jhones_sedex_btn_sim.pack(side="left", padx=5)
        self.jhones_sedex_btn_nao = ctk.CTkButton(buttons_frame, text="Não", width=70, fg_color=COLOR_BORDER_SELECTED if self.jhones_sedex_value_step is False else COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY, command=lambda event=None: self._set_jhones_sedex_value(False))
        self.jhones_sedex_btn_nao.pack(side="left", padx=5)

    def _build_step_notes_save(self):
        parent_frame = self._step_notes_save_frame
        for widget in parent_frame.winfo_children(): widget.destroy()
        ctk.CTkLabel(parent_frame, text="Notas Adicionais e Salvar", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(pady=(10,5))
        self.notes_text_step = ctk.CTkTextbox(parent_frame, height=80, width=400, fg_color=COLOR_BACKGROUND, text_color=COLOR_TEXT, border_color="gray50")
        self.notes_text_step.pack(pady=5)
        self.notes_text_step.bind("<KeyRelease>", self._check_secret_code)
        if 'notes' in self._current_match_data: self.notes_text_step.insert("1.0", self._current_match_data['notes'])
        if not hasattr(self, 'jhones_question_frame_step') or not self.jhones_question_frame_step.winfo_exists() or self.jhones_question_frame_step.master != self._step_notes_save_frame :
             self.jhones_question_frame_step = ctk.CTkFrame(parent_frame, fg_color="transparent")
        self._build_navigation_buttons(parent_frame, back_command=lambda: self._show_step(self._step_survivors_escaped_frame), next_command=self._action_final_save_match, next_text="Salvar Partida")
        self._check_secret_code()

    def _action_final_save_match(self, event=None):
        original_notes = self.notes_text_step.get("1.0", "end-1c").strip()
        notes_to_save = original_notes
        current_jhones_sedex_val = None

        if hasattr(self, 'jhones_question_frame_step') and self.jhones_question_frame_step.winfo_ismapped():
            if hasattr(self, 'jhones_sedex_value_step') and self.jhones_sedex_value_step is not None:
                current_jhones_sedex_val = bool(self.jhones_sedex_value_step)
            if original_notes.lower() == SECRET_CODE:
                notes_to_save = "" 
        
        self._current_match_data['notes'] = notes_to_save
        self._current_match_data['jhones_sedex'] = current_jhones_sedex_val
            
        if self._current_match_data.get('escaped') is True and self._current_match_data.get('survivors_escaped', 0) == 0:
            messagebox.showwarning("Dados Inválidos", "Se você sobreviveu, o número de sobreviventes que escaparam deve ser pelo menos 1.")
            return
        
        killer_id_val = self._current_match_data.get('killer_id')
        map_id_val = self._current_match_data.get('map_id')
        item_used_id_val = self._current_match_data.get('item_used_id')
        item_gained_id_val = self._current_match_data.get('item_gained_id')
        item_lost_id_val = self._current_match_data.get('item_lost_id')
        escaped_val = self._current_match_data.get('escaped')
        survivors_escaped_val = self._current_match_data.get('survivors_escaped')
        game_mode_val = self._current_match_data.get('game_mode')
        teammates_nicks_val = self._current_match_data.get('teammates', [])
        
        required_fields = {
            "ID do Assassino": killer_id_val, "ID do Mapa": map_id_val,
            "ID do Item Usado": item_used_id_val, "ID do Item Ganho": item_gained_id_val,
            "ID do Item Perdido": item_lost_id_val, "Status de Sobrevivência": escaped_val,
            "Nº de Sobrev. Escap.": survivors_escaped_val, "Modo 'Jogando'": game_mode_val
        }
        for field_name, field_value in required_fields.items():
            if field_value is None:
                messagebox.showerror("Erro de Dados", f"Valor ausente ou inválido para '{field_name}' antes de salvar.")
                return
            if field_name.endswith("_id") and not isinstance(field_value, int):
                 messagebox.showerror("Erro Interno de Tipo", f"Tipo inválido para '{field_name}'. Esperado int, recebeu {type(field_value)}.")
                 return
        
        if not isinstance(escaped_val, bool):
            messagebox.showerror("Erro Interno", f"Tipo inválido para 'escaped_val': {type(escaped_val)}")
            return
        if current_jhones_sedex_val is not None and not isinstance(current_jhones_sedex_val, bool):
            messagebox.showerror("Erro Interno", f"Tipo inválido para 'current_jhones_sedex_val': {type(current_jhones_sedex_val)}")
            return

        success = registrar_partida(
            killer_id=killer_id_val, map_id=map_id_val,
            item_used_id=item_used_id_val, item_gained_id=item_gained_id_val,
            item_lost_id=item_lost_id_val, escaped=escaped_val,
            survivors_escaped=survivors_escaped_val, notes=notes_to_save,
            game_mode=game_mode_val, teammates_nicks=teammates_nicks_val,
            jhones_sedex=current_jhones_sedex_val
        )
        if success:
            self.reset_match_registration()
            current_tab = self.tab_view.get()
            if current_tab == "Histórico": self.carregar_historico()
            if current_tab == "Estatísticas": self.carregar_estatisticas_view()


    def criar_aba_historico_content(self, tab_historico): 
        frame = ctk.CTkFrame(tab_historico, fg_color=COLOR_FRAME_BG)
        frame.pack(pady=10, padx=10, fill="both", expand=True)
        style = ttk.Style(); style.theme_use("default")
        style.configure("Treeview", background="#2B2B2B", foreground=COLOR_TEXT, fieldbackground="#2B2B2B", borderwidth=1, relief="solid")
        style.map('Treeview', background=[('selected', COLOR_PRIMARY_RED)])
        style.configure("Treeview.Heading", background="#1E1E1E", foreground=COLOR_TEXT, font=('Arial', 10, 'bold'), relief="flat")
        cols = ("ID", "Data", "Assassino", "Mapa", "Item Usado", "Item Ganho", "Item Perdido", "Sobreviveu?", "Sobrev. Escap.", "Jogando", "Companheiro(a)s", "Jhones Sedex", "Notas")
        self.history_tree = ttk.Treeview(frame, columns=cols, show='headings', style="Treeview")
        col_widths = {"ID": 30, "Data": 120, "Assassino": 140, "Mapa": 180, "Item Usado": 90, "Item Ganho": 90, "Item Perdido": 90, "Sobreviveu?": 80, "Sobrev. Escap.": 100, "Jogando": 70, "Companheiro(a)s": 120, "Jhones Sedex": 90, "Notas": 130}
        col_anchors = {"ID": 'center', "Sobreviveu?": 'center', "Sobrev. Escap.": 'center', "Jogando": 'center', "Jhones Sedex": 'center'}
        for col_name in cols:
            width = col_widths.get(col_name, 100); anchor = col_anchors.get(col_name, 'w')
            self.history_tree.heading(col_name, text=col_name); self.history_tree.column(col_name, width=width, anchor=anchor, minwidth=max(40,width-20))
        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.history_tree.yview); vsb.pack(side='right', fill='y')
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=self.history_tree.xview); hsb.pack(side='bottom', fill='x')
        self.history_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set); self.history_tree.pack(fill="both", expand=True)
        refresh_button = ctk.CTkButton(frame, text="Atualizar Histórico", command=self.carregar_historico, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY, text_color="#FFFFFF")
        refresh_button.pack(pady=10)

    def carregar_historico(self):
        if hasattr(self, 'history_tree') and self.history_tree and self.history_tree.winfo_exists():
            for i in self.history_tree.get_children(): self.history_tree.delete(i)
            partidas = buscar_historico_partidas()
            for partida_row in partidas:
                valores_formatados = []
                for valor in partida_row:
                    if isinstance(valor, (bytes, bytearray)):
                        valores_formatados.append(valor.decode('utf-8', errors='replace'))
                    else:
                        valores_formatados.append(valor)
                self.history_tree.insert("", "end", values=tuple(valores_formatados))

    def criar_aba_estatisticas_content(self, tab_estatisticas):
        self.stats_main_frame_container = ctk.CTkFrame(tab_estatisticas, fg_color=COLOR_FRAME_BG)
        self.stats_main_frame_container.pack(fill="both", expand=True)

        self.stats_controls_frame = ctk.CTkFrame(self.stats_main_frame_container, fg_color="transparent")
        self.stats_controls_frame.pack(pady=(10,5), padx=10, fill="x")
        
        self.refresh_stats_button = ctk.CTkButton(self.stats_controls_frame, text="Atualizar", command=self.carregar_estatisticas_view, fg_color=COLOR_BUTTON_SECONDARY, hover_color=COLOR_BUTTON_HOVER_SECONDARY)
        self.refresh_stats_button.pack(side="left", padx=(0,10))
        
        self.view_mode_button = ctk.CTkButton(self.stats_controls_frame, text="Ver Gráficos", command=self.toggle_stats_view, fg_color=COLOR_BUTTON_SECONDARY, hover_color=COLOR_BUTTON_HOVER_SECONDARY)
        self.view_mode_button.pack(side="left", padx=(0,10))
        
        self.stats_text_display_frame = ctk.CTkScrollableFrame(self.stats_main_frame_container, fg_color=COLOR_FRAME_BG)
        self.stats_text_display_frame.pack(fill="both", expand=True, padx=5, pady=(0,5)) 
        
        self.stats_charts_display_frame = ctk.CTkFrame(self.stats_main_frame_container, fg_color=COLOR_FRAME_BG) 
        
        self._build_text_stats_layout() 
        self.carregar_estatisticas_view() 

    def _build_text_stats_layout(self): 
        for widget in self.stats_text_display_frame.winfo_children(): widget.destroy()

        geral_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        geral_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(geral_frame, text="Estatísticas Gerais", font=ctk.CTkFont(size=18, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,8))
        self.total_partidas_label = ctk.CTkLabel(geral_frame, text="Total de Partidas Jogadas: N/A", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)); self.total_partidas_label.pack(anchor="w", pady=1)
        self.escapes_totais_label = ctk.CTkLabel(geral_frame, text="Total de partidas com escape: N/A", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)); self.escapes_totais_label.pack(anchor="w", pady=1)
        self.sacrificios_totais_label = ctk.CTkLabel(geral_frame, text="Total de partidas com sacrifício: N/A", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)); self.sacrificios_totais_label.pack(anchor="w", pady=1)
        
        self.taxa_sobrevivencia_frame = ctk.CTkFrame(geral_frame, fg_color="transparent")
        self.taxa_sobrevivencia_frame.pack(anchor="w", fill="x", pady=(5,0))
        self.taxa_sobrevivencia_label = ctk.CTkLabel(self.taxa_sobrevivencia_frame, text="Taxa de Sobrevivência Geral: N/A", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14, weight="bold")); self.taxa_sobrevivencia_label.pack(side="left", anchor="w")
        self.taxa_sobrevivencia_progressbar = ctk.CTkProgressBar(self.taxa_sobrevivencia_frame, width=200, height=15, progress_color=COLOR_PROGRESS_GREEN, fg_color=COLOR_FRAME_BG, border_color="gray40", border_width=1)
        self.taxa_sobrevivencia_progressbar.set(0)
        self.taxa_sobrevivencia_progressbar.pack(side="left", padx=10, anchor="w")
        
        self.killer_mais_enfrentado_label = ctk.CTkLabel(geral_frame, text="Killer mais enfrentado: N/A (0 vezes)", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)); self.killer_mais_enfrentado_label.pack(anchor="w", pady=1) 
        self.mapa_mais_jogado_label = ctk.CTkLabel(geral_frame, text="Mapa mais jogado: N/A (0 vezes)", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)); self.mapa_mais_jogado_label.pack(anchor="w", pady=1) 

        mode_stats_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        mode_stats_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(mode_stats_frame, text="Estatísticas de Jogando", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5))
        self.mode_stats_labels_frame = ctk.CTkFrame(mode_stats_frame, fg_color="transparent") 
        self.mode_stats_labels_frame.pack(fill="x")
        self.mode_stats_labels = {}
        for mode in GAME_MODES:
            self.mode_stats_labels[mode] = ctk.CTkLabel(self.mode_stats_labels_frame, text=f"Partidas no modo {mode}: 0 \t\t 0.00% de vezes escapada.", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14))
            self.mode_stats_labels[mode].pack(anchor="w", pady=1)

        ctk.CTkLabel(self.stats_text_display_frame, text="Por Assassino", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(pady=(15,5), padx=5, anchor="nw")
        self.killer_stats_display_frame = ctk.CTkScrollableFrame(self.stats_text_display_frame, fg_color=COLOR_FRAME_BG, height=200, border_color="gray30", border_width=1)
        self.killer_stats_display_frame.pack(pady=5, padx=5, fill="x", anchor="nw")
        
        teammate_stats_outer_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        teammate_stats_outer_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(teammate_stats_outer_frame, text="Estatísticas de Companheiro(a)s", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5))
        self.teammate_cards_display_frame = ctk.CTkScrollableFrame(teammate_stats_outer_frame, fg_color=COLOR_FRAME_BG, height=150, border_color="gray30", border_width=1) 
        self.teammate_cards_display_frame.pack(fill="x", expand=True, pady=(0,5))
        
        self.jhones_stats_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent") 
        ctk.CTkLabel(self.jhones_stats_frame, text="Estatísticas 'Jhones Sedex'", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5))
        self.jhones_sedex_stats_label = ctk.CTkLabel(self.jhones_stats_frame, text="Sim: 0 / Não: 0", justify="left", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14))
        self.jhones_sedex_stats_label.pack(anchor="w")
        
        items_detail_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        items_detail_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(items_detail_frame, text="Estatísticas de Itens", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5))
        self.items_levados_text_area = ctk.CTkTextbox(items_detail_frame, height=80, activate_scrollbars=True, wrap="none", fg_color=COLOR_BACKGROUND, text_color=COLOR_TEXT, border_color="gray50", font=ctk.CTkFont(size=13))
        ctk.CTkLabel(items_detail_frame, text="Itens Mais Levados:", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)).pack(anchor="w", pady=(5,0))
        self.items_levados_text_area.pack(fill="x", expand=True, pady=(0,10)); self.items_levados_text_area.insert("end", "Nenhum item levado registrado."); self.items_levados_text_area.configure(state="disabled")
        self.items_perdidos_text_area = ctk.CTkTextbox(items_detail_frame, height=80, activate_scrollbars=True, wrap="none", fg_color=COLOR_BACKGROUND, text_color=COLOR_TEXT, border_color="gray50", font=ctk.CTkFont(size=13))
        ctk.CTkLabel(items_detail_frame, text="Itens Mais Perdidos:", text_color=COLOR_TEXT, font=ctk.CTkFont(size=14)).pack(anchor="w", pady=(5,0))
        self.items_perdidos_text_area.pack(fill="x", expand=True); self.items_perdidos_text_area.insert("end", "Nenhum item perdido registrado."); self.items_perdidos_text_area.configure(state="disabled")
        
        map_stats_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        map_stats_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(map_stats_frame, text="Mapas mais jogados", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5))
        self.map_stats_text_area = ctk.CTkTextbox(map_stats_frame, height=100, activate_scrollbars=True, wrap="none", fg_color=COLOR_BACKGROUND, text_color=COLOR_TEXT, border_color="gray50", font=ctk.CTkFont(size=13))
        self.map_stats_text_area.pack(fill="x", expand=True); self.map_stats_text_area.insert("end", "Nenhuma partida registrada."); self.map_stats_text_area.configure(state="disabled")

        insights_frame = ctk.CTkFrame(self.stats_text_display_frame, fg_color="transparent")
        insights_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
        ctk.CTkLabel(insights_frame, text="Insights", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(anchor="w", pady=(0,5)) 
        self.insights_text_area = ctk.CTkTextbox(insights_frame, height=100, activate_scrollbars=True, wrap="word", fg_color=COLOR_BACKGROUND, text_color=COLOR_TEXT_SUBTLE, border_color="gray50", font=ctk.CTkFont(size=13, slant="italic"))
        self.insights_text_area.pack(fill="x", expand=True); self.insights_text_area.insert("end", "Nenhum insight disponível no momento."); self.insights_text_area.configure(state="disabled")


    def carregar_estatisticas_view(self): 
        try:
            stats = calcular_estatisticas_gerais()
            self._image_references.clear()
            self.total_partidas_label.configure(text=f"Total de Partidas Jogadas: {stats['total_partidas']}")
            self.escapes_totais_label.configure(text=f"Total de partidas com escape: {stats['escapes_totais']}") 
            total_sacrificios = stats['total_partidas'] - stats['escapes_totais']
            self.sacrificios_totais_label.configure(text=f"Total de partidas com sacrifício: {total_sacrificios}") 
            taxa_geral = stats['taxa_sobrevivencia_geral']
            self.taxa_sobrevivencia_label.configure(text=f"Taxa de Sobrevivência Geral: {taxa_geral:.2f}%")
            self.taxa_sobrevivencia_progressbar.set(taxa_geral / 100 if taxa_geral else 0) 
            if taxa_geral >= 60: self.taxa_sobrevivencia_progressbar.configure(progress_color=COLOR_PROGRESS_GREEN)
            elif taxa_geral >= 30: self.taxa_sobrevivencia_progressbar.configure(progress_color=COLOR_PROGRESS_ORANGE)
            else: self.taxa_sobrevivencia_progressbar.configure(progress_color=COLOR_PROGRESS_RED_BAR) 
            
            if hasattr(self, 'killer_mais_enfrentado_label'):
                k_nome = stats.get("killer_mais_enfrentado", {}).get("nome", "N/A")
                k_contagem = stats.get("killer_mais_enfrentado", {}).get("contagem", 0)
                self.killer_mais_enfrentado_label.configure(text=f"Killer mais enfrentado: {k_nome} ({k_contagem} {_formatar_contagem_vezes(k_contagem)})")
            
            if hasattr(self, 'mapa_mais_jogado_label'):
                m_nome = stats.get("mapa_mais_jogado", {}).get("nome", "N/A")
                m_contagem = stats.get("mapa_mais_jogado", {}).get("contagem", 0)
                self.mapa_mais_jogado_label.configure(text=f"Mapa mais jogado: {m_nome} ({m_contagem} {_formatar_contagem_vezes(m_contagem)})")

            for mode in GAME_MODES:
                if mode in stats["partidas_por_modo"]:
                    partidas = stats["partidas_por_modo"][mode]
                    taxa_sobrev_mode = stats["sobrevivencia_por_modo"].get(mode, 0.0)
                    self.mode_stats_labels[mode].configure(text=f"Partidas no modo {mode}: {partidas}\t\t{taxa_sobrev_mode:.2f}% de vezes escapada.")
                else:
                    self.mode_stats_labels[mode].configure(text=f"Partidas no modo {mode}: 0\t\t0.00% de vezes escapada.")
            
            for widget in self.killer_stats_display_frame.winfo_children(): widget.destroy()
            if stats["partidas_por_killer"]:
                sorted_killers = sorted(stats["partidas_por_killer"].items(), key=lambda item: (-item[1], item[0]))
                cols_killer_cards = 2; row_k, col_k = 0,0
                killer_cards_parent_frame = ctk.CTkFrame(self.killer_stats_display_frame, fg_color="transparent") 
                killer_cards_parent_frame.pack(fill="x")

                for killer_name, partidas in sorted_killers:
                    card = ctk.CTkFrame(killer_cards_parent_frame, fg_color=COLOR_FRAME_BG, border_width=1, border_color="gray30", corner_radius=8)
                    card.grid(row=row_k, column=col_k, padx=5, pady=5, sticky="nsew")
                    killer_cards_parent_frame.grid_columnconfigure(col_k % cols_killer_cards, weight=1) 

                    top_frame = ctk.CTkFrame(card, fg_color="transparent"); top_frame.pack(fill="x", padx=8, pady=(8,0))
                    image_name_base = killer_name.replace(" ", "_").replace(":", "")
                    ctk_image = self._load_image(image_name_base, PORTRAIT_SIZE_STATS, is_killer_portrait=True)
                    if ctk_image:
                        img_label = ctk.CTkLabel(top_frame, image=ctk_image, text="", fg_color="transparent"); img_label.pack(side="left", padx=(0,8))
                    else:
                        img_label = ctk.CTkLabel(top_frame, text="[S/I]", width=PORTRAIT_SIZE_STATS[0], height=PORTRAIT_SIZE_STATS[1], fg_color="gray20"); img_label.pack(side="left", padx=(0,8))
                    ctk.CTkLabel(top_frame, text=killer_name, font=ctk.CTkFont(size=14, weight="bold"), text_color=COLOR_TEXT).pack(side="left", anchor="w")
                    taxa_sobrev_killer = stats["sobrevivencia_por_killer"].get(killer_name, 0.0)
                    ctk.CTkLabel(card, text=f"{partidas} partida(s) – {taxa_sobrev_killer:.1f}% de escape", font=ctk.CTkFont(size=12), text_color=COLOR_TEXT_SUBTLE).pack(padx=8, pady=2, anchor="w")
                    progress_bar = ctk.CTkProgressBar(card, width=180, height=10, fg_color=COLOR_BACKGROUND, border_color="gray40", border_width=1)
                    progress_bar.set(taxa_sobrev_killer / 100 if taxa_sobrev_killer else 0)
                    if taxa_sobrev_killer >= 60: progress_bar.configure(progress_color=COLOR_PROGRESS_GREEN)
                    elif taxa_sobrev_killer >= 30: progress_bar.configure(progress_color=COLOR_PROGRESS_ORANGE)
                    else: progress_bar.configure(progress_color=COLOR_PROGRESS_RED_BAR)
                    progress_bar.pack(padx=8, pady=(0,8), fill="x")
                    col_k += 1
                    if col_k >= cols_killer_cards: col_k = 0; row_k += 1
                for i in range(cols_killer_cards): 
                    killer_cards_parent_frame.grid_columnconfigure(i, weight=1)
            else: ctk.CTkLabel(self.killer_stats_display_frame, text="Nenhuma partida registrada.", text_color=COLOR_TEXT).pack()
            
            for widget in self.teammate_cards_display_frame.winfo_children(): widget.destroy()
            if stats["sobrevivencia_com_teammate"]:
                sorted_teammates = sorted(stats["sobrevivencia_com_teammate"].items(), key=lambda item: -item[1]["partidas"])
                cols_teammate_cards = 2; row_t, col_t = 0,0
                teammate_cards_parent = ctk.CTkFrame(self.teammate_cards_display_frame, fg_color="transparent")
                teammate_cards_parent.pack(fill="x")
                for nick, data in sorted_teammates:
                    card = ctk.CTkFrame(teammate_cards_parent, fg_color=COLOR_FRAME_BG, border_width=1, border_color="gray30", corner_radius=8)
                    card.grid(row=row_t, column=col_t, padx=5, pady=5, sticky="nsew")
                    teammate_cards_parent.grid_columnconfigure(col_t % cols_teammate_cards, weight=1)
                    
                    ctk.CTkLabel(card, text=nick, font=ctk.CTkFont(size=14, weight="bold"), text_color=COLOR_TEXT).pack(padx=8, pady=(8,2), anchor="w")
                    ctk.CTkLabel(card, text=f"{data['partidas']} {_formatar_contagem_vezes(data['partidas'])} juntos – {data['taxa_escape']:.1f}% de escape", font=ctk.CTkFont(size=12), text_color=COLOR_TEXT_SUBTLE).pack(padx=8, pady=2, anchor="w")
                    
                    progress_bar_teammate = ctk.CTkProgressBar(card, width=180, height=10, fg_color=COLOR_BACKGROUND, border_color="gray40", border_width=1)
                    progress_bar_teammate.set(data['taxa_escape'] / 100 if data['taxa_escape'] else 0)
                    if data['taxa_escape'] >= 60: progress_bar_teammate.configure(progress_color=COLOR_PROGRESS_GREEN)
                    elif data['taxa_escape'] >=30: progress_bar_teammate.configure(progress_color=COLOR_PROGRESS_ORANGE)
                    else: progress_bar_teammate.configure(progress_color=COLOR_PROGRESS_RED_BAR)
                    progress_bar_teammate.pack(padx=8, pady=(0,8), fill="x")
                    col_t += 1
                    if col_t >= cols_teammate_cards: col_t = 0; row_t +=1
                for i in range(cols_teammate_cards): 
                    teammate_cards_parent.grid_columnconfigure(i, weight=1)
            else:
                ctk.CTkLabel(self.teammate_cards_display_frame, text="Nenhuma partida com companheiro(a)s registrada.", text_color=COLOR_TEXT).pack()

            if stats["partidas_com_jhones_respondido"] > 0:
                self.jhones_sedex_stats_label.configure(text=f"Sim: {stats['jhones_sedex_sim']} / Não: {stats['jhones_sedex_nao']}")
                if hasattr(self, 'jhones_stats_frame') and self.jhones_stats_frame.winfo_exists():
                    if not self.jhones_stats_frame.winfo_ismapped():
                        self.jhones_stats_frame.pack(pady=10, padx=5, fill="x", anchor="nw")
            else:
                if hasattr(self, 'jhones_stats_frame') and self.jhones_stats_frame.winfo_ismapped(): 
                    self.jhones_stats_frame.pack_forget()
            
            self.items_levados_text_area.configure(state="normal"); self.items_levados_text_area.delete("1.0", "end")
            if stats["itens_levados_count"]:
                for item, count in list(sorted(stats["itens_levados_count"].items(), key=lambda item: item[1], reverse=True))[:10]: 
                    self.items_levados_text_area.insert("end", f"- {item}: {count} {_formatar_contagem_vezes(count)}\n")
            else: self.items_levados_text_area.insert("end", "Nenhum item foi registrado como levado.")
            self.items_levados_text_area.configure(state="disabled")
            
            self.items_perdidos_text_area.configure(state="normal"); self.items_perdidos_text_area.delete("1.0", "end")
            if stats["itens_perdidos_count"]:
                for item, count in list(sorted(stats["itens_perdidos_count"].items(), key=lambda item: item[1], reverse=True))[:10]: 
                    self.items_perdidos_text_area.insert("end", f"- {item}: {count} {_formatar_contagem_vezes(count)}\n")
            else: self.items_perdidos_text_area.insert("end", "Nenhum item foi registrado como perdido.")
            self.items_perdidos_text_area.configure(state="disabled")
            
            self.map_stats_text_area.configure(state="normal"); self.map_stats_text_area.delete("1.0", "end")
            if stats["jogos_por_mapa"]:
                for mapa, count in list(sorted(stats["jogos_por_mapa"].items(), key=lambda item: item[1], reverse=True))[:10]: 
                    self.map_stats_text_area.insert("end", f"- {mapa}: {count} {_formatar_contagem_vezes(count)}\n")
            else: self.map_stats_text_area.insert("end", "Nenhuma partida registrada.")
            self.map_stats_text_area.configure(state="disabled")

            if hasattr(self, 'insights_text_area'):
                self.insights_text_area.configure(state="normal"); self.insights_text_area.delete("1.0", "end")
                insights = self._gerar_insights(stats) 
                if insights:
                    for insight in insights:
                        self.insights_text_area.insert("end", f"💡 {insight}\n\n")
                else:
                    self.insights_text_area.insert("end", "Nenhum insight disponível no momento. Jogue mais para desbloquear!")
                self.insights_text_area.configure(state="disabled")

            self.show_text_stats_view()
        except Exception as e:
            error_details = f"Erro ao carregar dados das estatísticas: {e}\n{traceback.format_exc()}"
            _log_error(error_details, "ganchometro_stats_error_log.txt")
            messagebox.showerror("Erro nas Estatísticas", f"Falha ao carregar os dados das estatísticas. Verifique o log 'ganchometro_stats_error_log.txt'.\nDetalhe: {e}")


    def _gerar_insights(self, stats_data): 
        insights_list = []
        if stats_data["total_partidas"] < 5: 
            return ["Registre mais algumas partidas para começar a ver curiosidades sobre seu jogo!"]

        if stats_data["sobrevivencia_por_killer"]:
            melhor_killer = None
            pior_killer = None
            max_taxa = -1
            min_taxa = 101
            
            for killer, taxa in stats_data["sobrevivencia_por_killer"].items():
                if stats_data["partidas_por_killer"].get(killer, 0) >= 3: 
                    if taxa > max_taxa:
                        max_taxa = taxa
                        melhor_killer = killer
                    if taxa < min_taxa:
                        min_taxa = taxa
                        pior_killer = killer
            
            if melhor_killer:
                insights_list.append(f"Você parece se dar bem contra '{melhor_killer}', com {max_taxa:.1f}% de escapes!")
            if pior_killer and pior_killer != melhor_killer:
                insights_list.append(f"'{pior_killer}' tem sido um desafio, com apenas {min_taxa:.1f}% de escapes. Que tal uma nova estratégia?")

        if stats_data["itens_levados_count"]:
            item_mais_levado = max(stats_data["itens_levados_count"], key=stats_data["itens_levados_count"].get, default=None)
            if item_mais_levado:
                contagem_levado = stats_data["itens_levados_count"][item_mais_levado]
                insights_list.append(f"Seu item queridinho parece ser '{item_mais_levado}', levado {contagem_levado} {_formatar_contagem_vezes(contagem_levado)}.")
                if item_mais_levado in stats_data["itens_perdidos_count"]:
                    contagem_perdido = stats_data["itens_perdidos_count"][item_mais_levado]
                    taxa_perda = (contagem_perdido / contagem_levado) * 100 if contagem_levado > 0 else 0
                    if taxa_perda > 50:
                        insights_list.append(f"Cuidado! Você perdeu '{item_mais_levado}' em {taxa_perda:.1f}% das vezes que o levou.")
        
        if stats_data["partidas_por_modo"]:
            modo_mais_jogado = max(stats_data["partidas_por_modo"], key=stats_data["partidas_por_modo"].get, default=None)
            if modo_mais_jogado:
                taxa_escape_modo = stats_data["sobrevivencia_por_modo"].get(modo_mais_jogado, 0)
                insights_list.append(f"Você joga mais no modo '{modo_mais_jogado}'. Sua taxa de escape nele é de {taxa_escape_modo:.1f}%.")

        if not insights_list:
            insights_list.append("Continue registrando suas partidas para mais curiosidades!")
            
        return random.sample(insights_list, k=min(len(insights_list), 2)) 

    def toggle_stats_view(self):
        if self.stats_text_display_frame.winfo_ismapped():
            self.show_charts_view()
        else:
            self.show_text_stats_view()

    def show_text_stats_view(self):
        try:
            if hasattr(self, 'stats_charts_display_frame') and self.stats_charts_display_frame.winfo_exists():
                self.stats_charts_display_frame.pack_forget()
            
            if hasattr(self, 'stats_text_display_frame') and self.stats_text_display_frame.winfo_exists():
                if not self.stats_text_display_frame.winfo_ismapped():
                    self.stats_text_display_frame.pack(fill="both", expand=True, padx=5, pady=(0,5))
            
            if hasattr(self, 'view_mode_button') and self.view_mode_button.winfo_exists():
                self.view_mode_button.configure(text="Ver Gráficos")
        except Exception as e:
            _log_error(f"Erro em show_text_stats_view: {e}\n{traceback.format_exc()}", "ganchometro_stats_error_log.txt")


    def show_charts_view(self):
        if self.stats_text_display_frame.winfo_ismapped(): 
            self.stats_text_display_frame.pack_forget()
        if not self.stats_charts_display_frame.winfo_ismapped():
            self.stats_charts_display_frame.pack(fill="both", expand=True, padx=5, pady=(0,5))
        self.view_mode_button.configure(text="Ver Texto")
        self._populate_charts_frame()

    def _populate_charts_frame(self):
        try:
            for widget in self.stats_charts_display_frame.winfo_children(): widget.destroy()
            stats = calcular_estatisticas_gerais()
            if not stats["total_partidas"]:
                ctk.CTkLabel(self.stats_charts_display_frame, text="Não há dados suficientes para gerar gráficos.", font=ctk.CTkFont(size=16), text_color=COLOR_TEXT).pack(expand=True); return
            
            charts_internal_tab_view = ctk.CTkTabview(self.stats_charts_display_frame, fg_color=COLOR_FRAME_BG, segmented_button_selected_color=COLOR_PRIMARY_RED, text_color=COLOR_TEXT, border_color=COLOR_FRAME_BG, border_width=0)
            charts_internal_tab_view.pack(fill="both", expand=True, padx=0, pady=0)
            
            def _criar_grafico_barras_horizontais(master_tab, dados, titulo, cor_barra=COLOR_SECONDARY_RED, top_n=5):
                if not dados:
                    ctk.CTkLabel(master_tab, text="Sem dados suficientes para este gráfico.", text_color=COLOR_TEXT).pack(expand=True)
                    return
                
                nomes = [item[0] for item in dados[:top_n]]
                contagens = [item[1] for item in dados[:top_n]]
                
                if not nomes: 
                    ctk.CTkLabel(master_tab, text="Sem dados suficientes para este gráfico.", text_color=COLOR_TEXT).pack(expand=True)
                    return

                plt.style.use('dark_background')
                fig, ax = plt.subplots(figsize=(7, max(3, len(nomes) * 0.6))) 
                bars = ax.barh(nomes, contagens, color=cor_barra, edgecolor=COLOR_TEXT)
                ax.set_xlabel('Contagem', color=COLOR_TEXT, fontsize=10)
                ax.set_title(titulo, color=COLOR_TEXT, fontsize=12)
                ax.tick_params(axis='x', colors=COLOR_TEXT, labelsize=9)
                ax.tick_params(axis='y', colors=COLOR_TEXT, labelsize=9)
                ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=5, prune='both'))
                fig.patch.set_facecolor(COLOR_BACKGROUND)
                ax.set_facecolor(COLOR_FRAME_BG)
                ax.invert_yaxis() 

                for bar in bars:
                    width = bar.get_width()
                    ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, f'{width}', ha='left', va='center', color=COLOR_TEXT, fontsize=8)
                
                plt.tight_layout(pad=1.5)
                canvas = FigureCanvasTkAgg(fig, master=master_tab)
                canvas.get_tk_widget().pack(fill="both", expand=True)
                canvas.draw()
                plt.close(fig)

            tab_killer = charts_internal_tab_view.add("Por Assassino")
            if stats["partidas_por_killer"]:
                partidas_por_killer_sorted = sorted(stats["partidas_por_killer"].items(), key=lambda item: item[1], reverse=True)[:10]
                killers_top = [item[0] for item in partidas_por_killer_sorted]
                survival_rates_top = [stats["sobrevivencia_por_killer"].get(k, 0) for k in killers_top]
                num_matches_top = [stats["partidas_por_killer"].get(k,0) for k in killers_top]
                if killers_top:
                    plt.style.use('dark_background'); fig1, ax1 = plt.subplots(figsize=(8, 5.5));
                    bars1 = ax1.bar(killers_top, survival_rates_top, color=COLOR_SECONDARY_RED, edgecolor=COLOR_TEXT, width=0.6)
                    ax1.set_ylabel('Taxa de Sobrevivência (%)', color=COLOR_TEXT, fontsize=10); ax1.set_title('Top Assassinos vs Taxa de Sobrevivência', color=COLOR_TEXT, fontsize=12)
                    ax1.set_xticks(range(len(killers_top)))
                    ax1.set_xticklabels(killers_top, rotation=45, ha="right", color=COLOR_TEXT, fontsize=9)
                    ax1.tick_params(axis='y', colors=COLOR_TEXT, labelsize=9)
                    ax1.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5, prune='both')); ax1.set_ylim(0, 100)
                    fig1.patch.set_facecolor(COLOR_BACKGROUND); ax1.set_facecolor(COLOR_FRAME_BG)
                    for i, bar in enumerate(bars1):
                        yval = bar.get_height(); text_y_pos = yval / 2 if yval > 20 else yval + 3
                        text_color_bar = 'white' if yval > 20 else COLOR_TEXT
                        va_align = 'center' if yval > 20 else 'bottom'
                        ax1.text(bar.get_x() + bar.get_width()/2.0, text_y_pos, f"{num_matches_top[i]}p\n{yval:.0f}%", ha='center', va=va_align, fontsize=7, color=text_color_bar, weight='bold')
                    plt.tight_layout(pad=2.0); canvas1 = FigureCanvasTkAgg(fig1, master=tab_killer); canvas1.get_tk_widget().pack(fill="both", expand=True); canvas1.draw(); plt.close(fig1)
                else: ctk.CTkLabel(tab_killer, text="Sem dados suficientes.", text_color=COLOR_TEXT).pack()
            else: ctk.CTkLabel(tab_killer, text="Sem dados de partidas por assassino.", text_color=COLOR_TEXT).pack()

            tab_modo = charts_internal_tab_view.add("Por 'Jogando'")
            if stats["partidas_por_modo"]:
                modes, counts = [], []
                for mode_name in GAME_MODES:
                    if mode_name in stats["partidas_por_modo"] and stats["partidas_por_modo"][mode_name] > 0:
                        modes.append(mode_name); counts.append(stats["partidas_por_modo"][mode_name])
                if modes and counts:
                    plt.style.use('dark_background'); fig2, ax2 = plt.subplots(figsize=(7, 5));
                    pie_colors = [COLOR_SECONDARY_RED, "#5A0000", "#C04000", "#777777"];
                    wedges, texts, autotexts = ax2.pie(counts, labels=modes, autopct='%1.1f%%', startangle=90, colors=pie_colors[:len(modes)], textprops={'color': COLOR_TEXT, 'fontsize':10,'weight':'bold'})
                    for autotext in autotexts: autotext.set_color('white'); autotext.set_fontsize(9)
                    ax2.axis('equal'); ax2.set_title("Distribuição de Partidas por 'Jogando'", color=COLOR_TEXT, fontsize=12)
                    fig2.patch.set_facecolor(COLOR_BACKGROUND); plt.tight_layout(pad=1.5); canvas2 = FigureCanvasTkAgg(fig2, master=tab_modo); canvas2.get_tk_widget().pack(fill="both", expand=True); canvas2.draw(); plt.close(fig2)
                else: ctk.CTkLabel(tab_modo, text="Sem dados suficientes (modos com 0 partidas não são exibidos).", text_color=COLOR_TEXT).pack()
            else: ctk.CTkLabel(tab_modo, text="Sem dados de partidas por modo de jogo.", text_color=COLOR_TEXT).pack()

            tab_mapas = charts_internal_tab_view.add("Mapas mais jogados") 
            mapas_ordenados = sorted(stats["jogos_por_mapa"].items(), key=lambda item: item[1], reverse=True)
            _criar_grafico_barras_horizontais(tab_mapas, mapas_ordenados, "Top Mapas Mais Jogados", COLOR_PROGRESS_GREEN)

            tab_itens_levados = charts_internal_tab_view.add("Itens Mais Levados")
            itens_levados_ordenados = sorted(stats["itens_levados_count"].items(), key=lambda item: item[1], reverse=True)
            _criar_grafico_barras_horizontais(tab_itens_levados, itens_levados_ordenados, "Top Itens Mais Levados", COLOR_PROGRESS_ORANGE)
            
            tab_itens_perdidos = charts_internal_tab_view.add("Itens Mais Perdidos")
            itens_perdidos_ordenados = sorted(stats["itens_perdidos_count"].items(), key=lambda item: item[1], reverse=True)
            _criar_grafico_barras_horizontais(tab_itens_perdidos, itens_perdidos_ordenados, "Top Itens Mais Perdidos", COLOR_PROGRESS_RED_BAR)

            tab_companheiros = charts_internal_tab_view.add("Sobrev. c/ Companheiros")
            if stats["sobrevivencia_com_teammate"]:
                dados_companheiros = []
                for nick, data in stats["sobrevivencia_com_teammate"].items():
                    if data["partidas"] > 0 : 
                        dados_companheiros.append((nick, data["taxa_escape"]))
                
                dados_companheiros_sorted = sorted(dados_companheiros, key=lambda item: item[1], reverse=True)[:10] 
                
                if dados_companheiros_sorted:
                    nomes_comp = [item[0] for item in dados_companheiros_sorted]
                    taxas_comp = [item[1] for item in dados_companheiros_sorted]
                    
                    plt.style.use('dark_background'); fig_comp, ax_comp = plt.subplots(figsize=(8, max(3, len(nomes_comp) * 0.5)))
                    bars_comp = ax_comp.barh(nomes_comp, taxas_comp, color=COLOR_PROGRESS_GREEN, edgecolor=COLOR_TEXT)
                    ax_comp.set_xlabel('Taxa de Escape (%)', color=COLOR_TEXT, fontsize=10)
                    ax_comp.set_title('Taxa de Escape com Companheiros', color=COLOR_TEXT, fontsize=12)
                    ax_comp.tick_params(axis='x', colors=COLOR_TEXT, labelsize=9)
                    ax_comp.tick_params(axis='y', colors=COLOR_TEXT, labelsize=9)
                    ax_comp.set_xlim(0, 100)
                    ax_comp.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=5, prune='both'))
                    fig_comp.patch.set_facecolor(COLOR_BACKGROUND); ax_comp.set_facecolor(COLOR_FRAME_BG)
                    ax_comp.invert_yaxis()
                    for bar in bars_comp:
                        width = bar.get_width()
                        ax_comp.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', ha='left', va='center', color=COLOR_TEXT, fontsize=8)
                    plt.tight_layout(pad=1.5)
                    canvas_comp = FigureCanvasTkAgg(fig_comp, master=tab_companheiros)
                    canvas_comp.get_tk_widget().pack(fill="both", expand=True); canvas_comp.draw(); plt.close(fig_comp)
                else:
                    ctk.CTkLabel(tab_companheiros, text="Sem dados suficientes para este gráfico.", text_color=COLOR_TEXT).pack(expand=True)
            else:
                ctk.CTkLabel(tab_companheiros, text="Sem dados de partidas com companheiros.", text_color=COLOR_TEXT).pack(expand=True)

            tab_jhones = charts_internal_tab_view.add("Jhones Sedex")
            if stats["partidas_com_jhones_respondido"] > 0:
                labels_jhones = ['Sim', 'Não']
                sizes_jhones = [stats["jhones_sedex_sim"], stats["jhones_sedex_nao"]]
                colors_jhones = [COLOR_PROGRESS_GREEN, COLOR_PROGRESS_RED_BAR]
                
                if stats["jhones_sedex_sim"] > 0 or stats["jhones_sedex_nao"] > 0:
                    plt.style.use('dark_background'); fig_j, ax_j = plt.subplots(figsize=(6, 4))
                    wedges_j, texts_j, autotexts_j = ax_j.pie(sizes_jhones, labels=labels_jhones, autopct='%1.1f%%', startangle=90, colors=colors_jhones, textprops={'color': COLOR_TEXT, 'weight':'bold'})
                    for autotext in autotexts_j: autotext.set_color('white')
                    ax_j.axis('equal'); ax_j.set_title("Jhones deu Sedex?", color=COLOR_TEXT, fontsize=12)
                    fig_j.patch.set_facecolor(COLOR_BACKGROUND)
                    plt.tight_layout(pad=1.0); canvas_j = FigureCanvasTkAgg(fig_j, master=tab_jhones)
                    canvas_j.get_tk_widget().pack(fill="both", expand=True); canvas_j.draw(); plt.close(fig_j)
                else:
                     ctk.CTkLabel(tab_jhones, text="Sem dados de 'Sim' ou 'Não' para o Jhones Sedex.", text_color=COLOR_TEXT).pack(expand=True)
            else:
                ctk.CTkLabel(tab_jhones, text="Nenhuma partida com informação 'Jhones Sedex' registrada.", text_color=COLOR_TEXT).pack(expand=True)

        except Exception as e:
            error_details = f"Erro ao popular gráficos de estatísticas: {e}\n{traceback.format_exc()}"
            _log_error(error_details, "ganchometro_stats_error_log.txt")
            messagebox.showerror("Erro nos Gráficos", f"Falha ao gerar os gráficos. Verifique o log 'ganchometro_stats_error_log.txt'.\nDetalhe: {e}")
            ctk.CTkLabel(self.stats_charts_display_frame, text=f"Erro ao gerar gráficos: {e}", text_color="orange", wraplength=self.stats_charts_display_frame.winfo_width()-20).pack(expand=True, fill="both")


    def criar_aba_gerenciar_dados_content(self, tab_gerenciar_dados): 
        frame = ctk.CTkFrame(tab_gerenciar_dados, fg_color=COLOR_FRAME_BG)
        frame.pack(pady=20, padx=20, fill="both", expand=True)
        ctk.CTkLabel(frame, text="Gerenciamento de Dados das Partidas", font=ctk.CTkFont(size=16, weight="bold"), text_color=COLOR_TEXT).pack(pady=(0,20), anchor="center")
        export_button = ctk.CTkButton(frame, text="Exportar Dados das Partidas", command=self.exportar_dados_action, width=250, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY, text_color="#FFFFFF")
        export_button.pack(pady=10, anchor="center")
        ctk.CTkLabel(frame, text="Exporta todas as partidas para um arquivo JSON.", font=ctk.CTkFont(size=12), text_color="gray60").pack(pady=(0,20), anchor="center")
        import_button = ctk.CTkButton(frame, text="Importar Dados das Partidas", command=self.importar_dados_action, width=250, fg_color=COLOR_BUTTON_PRIMARY, hover_color=COLOR_BUTTON_HOVER_PRIMARY, text_color="#FFFFFF")
        import_button.pack(pady=10, anchor="center")
        ctk.CTkLabel(frame, text="Importa partidas de um arquivo JSON.", font=ctk.CTkFont(size=12), text_color="gray60", justify="center").pack(pady=(0,10), anchor="center")

    def exportar_dados_action(self):
        conn, cursor = conectar_db()
        try:
            cursor.execute('''
                SELECT m.id, m.match_date, k.name AS killer_name, mp.name AS map_name, iu.name AS item_used_name, ig.name AS item_gained_name,
                       il.name AS item_lost_name, m.escaped, m.survivors_escaped, m.game_mode, m.notes, m.jhones_sedex
                FROM matches m LEFT JOIN killers k ON m.killer_id = k.id LEFT JOIN maps mp ON m.map_id = mp.id
                LEFT JOIN items iu ON m.item_used_id = iu.id LEFT JOIN items ig ON m.item_gained_id = ig.id
                LEFT JOIN items il ON m.item_lost_id = il.id ORDER BY m.match_date ASC
            ''')
            column_names = [desc[0] for desc in cursor.description]
            all_matches_data = [dict(zip(column_names, row)) for row in cursor.fetchall()]
            if not all_matches_data: messagebox.showinfo("Exportar Dados", "Nenhuma partida para exportar."); return
            for match_data in all_matches_data:
                cursor.execute('SELECT t.nickname FROM teammates t JOIN match_teammates mt ON t.id = mt.teammate_id WHERE mt.match_id = ?', (match_data['id'],))
                match_data['teammates_nicks'] = [row[0] for row in cursor.fetchall()]; del match_data['id']
            filepath = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")], title="Salvar Dados como...")
            if filepath:
                with open(filepath, 'w', encoding='utf-8') as f: json.dump(all_matches_data, f, ensure_ascii=False, indent=4)
                messagebox.showinfo("Exportar Dados", f"Dados exportados para:\n{filepath}")
        except Exception as e: 
            error_details = f"Erro ao exportar dados: {e}\n{traceback.format_exc()}"
            _log_error(error_details, "ganchometro_export_error.txt")
            messagebox.showerror("Erro de Exportação", f"Ocorreu um erro. Verifique o log 'ganchometro_export_error.txt'.\nDetalhe: {e}")
        finally: 
            if conn:
                conn.close()

    def importar_dados_action(self):
        filepath = filedialog.askopenfilename(filetypes=[("JSON files", "*.json"), ("All files", "*.*")], title="Abrir Arquivo de Dados...")
        if not filepath: return
        try:
            with open(filepath, 'r', encoding='utf-8') as f: partidas_importadas = json.load(f)
        except Exception as e: 
            error_details = f"Erro ao ler arquivo de importação: {e}\n{traceback.format_exc()}"
            _log_error(error_details, "ganchometro_import_error.txt")
            messagebox.showerror("Erro de Importação", f"Não foi possível ler o arquivo. Verifique o log 'ganchometro_import_error.txt'.\nDetalhe: {e}"); return
        if not isinstance(partidas_importadas, list): 
            _log_error(f"Formato de arquivo de importação inválido. Esperado: lista JSON. Recebido: {type(partidas_importadas)}", "ganchometro_import_error.txt")
            messagebox.showerror("Erro de Importação", "Formato de arquivo inválido."); return
        
        conn, cursor = conectar_db()
        importadas_count, puladas_count, erros_count = 0,0,0
        killer_id_cache = {k_name: k_id for k_id, k_name in buscar_items_genericos("killers")}
        map_id_cache = {m_name: m_id for m_id, m_name in buscar_items_genericos("maps")} 
        item_id_cache = {i_name: i_id for i_id, i_name in buscar_items_genericos("items")}
        
        for partida_data in partidas_importadas:
            try:
                killer_name=partida_data.get("killer_name"); map_name=partida_data.get("map_name"); item_used_name=partida_data.get("item_used_name")
                item_gained_name=partida_data.get("item_gained_name"); item_lost_name=partida_data.get("item_lost_name"); game_mode=partida_data.get("game_mode")
                teammates_nicks_import = partida_data.get("teammates_nicks", []); jhones_sedex = partida_data.get("jhones_sedex")
                if game_mode not in GAME_MODES and game_mode is not None: game_mode = None
                killer_id=killer_id_cache.get(killer_name); map_id=map_id_cache.get(map_name); item_used_id=item_id_cache.get(item_used_name)
                item_gained_id=item_id_cache.get(item_gained_name); item_lost_id=item_id_cache.get(item_lost_name)
                if None in [killer_id, map_id, item_used_id, item_gained_id, item_lost_id]: puladas_count += 1; continue
                escaped_val = partida_data.get("escaped");
                escaped = escaped_val.lower() == 'true' if isinstance(escaped_val, str) else bool(escaped_val)
                success = registrar_partida(killer_id, map_id, item_used_id, item_gained_id, item_lost_id, escaped,
                                        int(partida_data.get("survivors_escaped",0)), partida_data.get("notes",""), game_mode,
                                        teammates_nicks_import, jhones_sedex, match_date_str=partida_data.get("match_date"))
                if success: importadas_count += 1
                else: erros_count += 1
            except Exception as e: 
                error_details = f"Erro ao importar partida: {e} - Dados: {partida_data}\n{traceback.format_exc()}"
                _log_error(error_details, "ganchometro_import_error.txt")
                erros_count += 1; continue
        if conn: conn.close()
        messagebox.showinfo("Resultado Importação", f"Importadas: {importadas_count}\nPuladas: {puladas_count}\nCom Erro: {erros_count}\nVerifique 'ganchometro_import_error.txt' para detalhes dos erros.")
        if self._current_page_name == "Histórico": self.carregar_historico()
        if self._current_page_name == "Estatísticas": self.carregar_estatisticas_view()

    def criar_aba_sobre_content(self, tab_sobre): 
        frame = ctk.CTkFrame(tab_sobre, fg_color=COLOR_FRAME_BG)
        frame.pack(pady=20, padx=20, fill="both", expand=True)
        ctk.CTkLabel(frame, text=APP_NAME, font=ctk.CTkFont(size=24, weight="bold"), text_color=COLOR_PRIMARY_RED).pack(pady=(10,5))
        ctk.CTkLabel(frame, text=f"Versão {APP_VERSION}", font=ctk.CTkFont(size=12), text_color="gray60").pack(pady=(0,20))
        ctk.CTkLabel(frame, text="Uma ferramenta para registrar e analisar suas partidas de Dead by Daylight.", font=ctk.CTkFont(size=14), wraplength=400, justify="center", text_color=COLOR_TEXT).pack(pady=10)
        ctk.CTkLabel(frame, text="Feito por: @brina-chan", font=ctk.CTkFont(size=14, weight="bold"), text_color=COLOR_TEXT).pack(pady=(20,5))
        github_link = "https://github.com/brina-chan/Ganchometro" 
        update_button = ctk.CTkButton(frame, 
                                      text="Verificar atualizações", 
                                      command=lambda: webbrowser.open_new_tab(github_link),
                                      fg_color=COLOR_PRIMARY_RED, 
                                      hover_color=COLOR_SECONDARY_RED,
                                      text_color="#FFFFFF") # Assegura bom contraste para o texto do botão
        update_button.pack(pady=10)
        ctk.CTkLabel(frame, text="Ícones de personagens e outros elementos visuais são propriedade da Behaviour Interactive.", font=ctk.CTkFont(size=10), text_color="gray50", wraplength=450, justify="center").pack(pady=(30,10), side="bottom")

    def _generate_stats_page_log(self):
        log_messages = [f"--- Log de Status da Aba Estatísticas: {datetime.datetime.now()} ---"]
        try:
            log_messages.append(f"Frame de texto 'stats_text_display_frame' existe: {hasattr(self, 'stats_text_display_frame')}")
            if hasattr(self, 'stats_text_display_frame') and self.stats_text_display_frame.winfo_exists():
                log_messages.append(f"  stats_text_display_frame é visível (ismapped): {self.stats_text_display_frame.winfo_ismapped()}")
                log_messages.append(f"  stats_text_display_frame filhos: {self.stats_text_display_frame.winfo_children()}")
                if hasattr(self, 'total_partidas_label') and self.total_partidas_label.winfo_exists():
                    log_messages.append(f"    total_partidas_label texto: '{self.total_partidas_label.cget('text')}'")
                else:
                    log_messages.append(f"    total_partidas_label não existe ou foi destruído.")
            else:
                log_messages.append(f"  stats_text_display_frame não existe ou foi destruído.")
            
            log_messages.append(f"Frame de gráficos 'stats_charts_display_frame' existe: {hasattr(self, 'stats_charts_display_frame')}")
            if hasattr(self, 'stats_charts_display_frame') and self.stats_charts_display_frame.winfo_exists():
                log_messages.append(f"  stats_charts_display_frame é visível (ismapped): {self.stats_charts_display_frame.winfo_ismapped()}")
            else:
                log_messages.append(f"  stats_charts_display_frame não existe ou foi destruído.")

            stats_data = calcular_estatisticas_gerais() 
            log_messages.append(f"\nDados calculados em calcular_estatisticas_gerais():")
            for key, value in stats_data.items():
                log_messages.append(f"  {key}: {value}")

        except Exception as e:
            log_messages.append(f"\nERRO ao gerar log de status: {e}\n{traceback.format_exc()}")
        
        _log_error("\n".join(log_messages), "ganchometro_stats_debug_log.txt")
        messagebox.showinfo("Log Gerado", "Log de status da página de estatísticas foi gerado em 'ganchometro_stats_debug_log.txt'.")


if __name__ == "__main__":
    try:
        ctk.set_appearance_mode("Dark")
        app = DBDTrackerApp()
        app.mainloop()
    except Exception as e:
        error_message = f"Ocorreu um erro fatal na inicialização ou execução principal:\n\n{str(e)}\n\n{traceback.format_exc()}"
        print(error_message) 
        _log_error(error_message, "ganchometro_CRASH_log.txt") 
        
        try:
            import tkinter as tk_basic 
            root_err_msg = tk_basic.Tk() 
            root_err_msg.withdraw()
            messagebox.showerror("Erro Fatal no Ganchômetro", f"{error_message}\n\nUm log detalhado PODE ter sido salvo em 'ganchometro_CRASH_log.txt'.\nPor favor, verifique a consola para mais detalhes.")
            root_err_msg.destroy()
        except Exception as e_msg: 
            print(f"Erro adicional ao tentar mostrar a messagebox de erro fatal: {e_msg}")

