from scraper_object import *
import time
from scraper.sql import *
class Scraper_Runner():
    def __init__(self, matricule_df, ids, scraper, batch_size, start, stop):
        self.matricule_df = matricule_df
        self.ids = ids
        self.scraper = scraper
        self.batch_size = batch_size
        self.start = start
        self.stop = stop
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.db_file = f"sql/{self.timestr}/mtl_properties.sqlite"
        self.valid_save_queue = list()
        self.error_save_queue = list()
        self.scraped_ids = set()

    def run(self):
        make_dirs(self.timestr)
        batches = range(self.start, self.stop, self.batch_size)
        print(f"Initializing session with batch size {self.batch_size} and upper limit {self.stop}.")
        create_table(self.db_file)
        ids_to_scrape = dict()
        for idx, lower in enumerate(batches):
            t1 = time.perf_counter()
            print(f"PROCESSING BATCH {idx} FROM {math.ceil((self.stop-self.start) / self.batch_size)}.")
            upper = min(lower + self.batch_size, self.stop)
            stop_index = lower + self.batch_size if lower + self.batch_size < upper else upper
            ids_to_scrape= self.ids[lower:upper]
            
            try:
                status = list(self.thread(ids_to_scrape))
            except requests.exceptions.ConnectionError:
                time.sleep(20*60)
            #print("printing status",status)

            #successful and unsuccessful scrapes are saved here to avoid concurrency issues
            self.apply_valid_saves()
            self.apply_error_saves()

            if status.count(False) > 50:
                break
                print("Too many fails in status count.")
            t2 = time.perf_counter()
            print(f"Processed {self.batch_size} urls. Ran in {t2-t1} seconds at a speed of {self.batch_size/(t2-t1)} entries per second")

    def thread(self, ids_to_scrape, url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
        ):
        #print(type(ids_to_scrape))
        # matricule = list(ids_to_scrape.keys())
        # data = list(ids_to_scrape.values())
        with concurrent.futures.ThreadPoolExecutor(70) as executor:
            status = executor.map(self.fetch_url_multi, ids_to_scrape)
            
            #status = executor.map(self.fetch_url_multi, matricule, data)
        return status

    def apply_valid_saves(self):
        print("Saving batch")
        count = 0
        #for save in self.valid_save_queue:
            # count = count + 1
            # if(count % 100 == 0):
            #     print("Saved ", count, "items")
            # if os.path.isfile(self.db_file):
            #     save_to_sql(self.db_file, save[0], save[1])
            #     #print(f"Saved ID {matricule} to db.")
        save_batch_to_sql(self.db_file, self.valid_save_queue)
        for save in self.valid_save_queue:
            with open(f"logs/{self.timestr}/scraped.txt", "a") as f:
                f.write(f"{save[0]}\n")
        self.valid_save_queue = list()
    def apply_error_saves(self):
        for error in self.error_save_queue:
            with open(f"logs/{self.timestr}/failed.txt", "a") as f:
                    f.write(f"{error}\n")

    def fetch_url_multi(self, id_to_scrape, url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"):
        #print(ids_to_scrape)
        #matricule = list(ids_to_scrape.keys())
        #print("Matricule: ", matricule)
        #print("Data: ", data)
        #exit(0)
        #data = list(ids_to_scrape.values())
        response = self.scraper.session.post(url, data = id_to_scrape)
        status = True
        matricule = self.id_To_matricule(id_to_scrape)
        if "Rechercher par" not in response.text:
            #print(f"Successfully opened page for ID {matricule}.")
            binary = response.text.encode('utf8')
            if(matricule not in self.scraped_ids):
                self.valid_save_queue.append([matricule,binary])
                self.scraped_ids.add(matricule)
            else:
                print("Error! Already processed matricule: ", matricule)
            # if os.path.isfile(self.db_file):
            #     save_to_sql(self.db_file, matricule, binary)
            #     #print(f"Saved ID {matricule} to db.")
            #     with open(f"logs/{self.timestr}/scraped.txt", "a") as f:
            #         f.write(f"{matricule}\n")
        else:
            print(f"Failed to load page for ID {matricule}. Check if session expired.")
            status = False

            self.error_save_queue.append(matricule)
            # with open(f"logs/{self.timestr}/failed.txt", "a") as f:
            #     f.write(f"{matricule}\n")
        return status
    def resume_scraping(self, prev_time_str):
        make_dirs(self.timestr)
        self.remove_prev_scraped_ids(prev_time_str)
        
        batches = range(self.start, self.stop, self.batch_size)
        print(f"Initializing session with batch size {self.batch_size} and upper limit {self.stop}.")

        #renames old file. effictively moves old file to new scraping location
        os.rename(f"sql\{prev_time_str}\mtl_properties.sqlite", f"sql\{self.timestr}\mtl_properties.sqlite")
        #create_table(self.db_file)
        
        for idx, lower in enumerate(batches):
            t1 = time.perf_counter()
            print(f"PROCESSING BATCH {idx} FROM {math.ceil((self.stop-self.start) / self.batch_size)}.")
            upper = min(lower + self.batch_size, self.stop)
            ids_to_scrape = list(self.ids.items())
            ids_to_scrape = ids_to_scrape[lower:upper]
            try:
                status = list(self.thread(ids_to_scrape))
            except requests.exceptions.ConnectionError:
                time.sleep(20*60)
            if status.count(False) > 50:
                break
                print("Too many fails in status count.")
            t2 = time.perf_counter()
            print(f"Processed {self.batch_size} urls. Ran in {t2-t1} seconds at a speed of {self.batch_size/(t2-t1)} entries per second")
        
    
    def remove_prev_scraped_ids(self, prev_time_str):
        prev_scraped_ids = list()
        count = 1
        with open(f"logs/{prev_time_str}/scraped.txt", "r") as prev_file:
            t1 = time.perf_counter()
            for row in prev_file:
                prev_scraped_ids.append(row)
                if(count % 100000 == 0):
                    t2 = time.perf_counter()
                    
                    #print(count)
                    print(f"Initialized 100000 ids in {t2-t1} seconds")
                    t1 = time.perf_counter()
                count = count + 1
        print("initialized prev ids")

        count = 1
        t1 = time.perf_counter()
        #print(self.ids)
        for i in prev_scraped_ids:
            i = i.replace('\n', '') 
            if(self.ids.get(i) != None ):
                self.scraped_ids.add(self.ids.pop(i))
            if(count % 10000 == 0):
                t2 = time.perf_counter()
                print(f"Traversed 100000 ids in {t2-t1} seconds")
                t1 = time.perf_counter()
            count = count + 1
        print("removed prev ids")

        self.stop = len(self.ids)

        with open(f"logs/{self.timestr}/scraped.txt", "a") as new_file:
            for i in prev_scraped_ids:
                new_file.write(f"{i}")

    def id_To_matricule(self, id_to_scrape):
        matricule = ""
        #print(id_to_scrape)
        for key in id_to_scrape:
            #print(id_to_scrape[key])
            matricule = matricule + id_to_scrape[key] + "-"

        #remove extra "-"
        matricule = matricule[:-1]
        return matricule

def main():
    # with open(f"logs/20200725-142521/scraped.txt", "r") as old_file:
    #     with open(f"logs/20200725-142521/scraped_new.txt", "w") as new_file:
    #         count = 0
    #         for i in old_file:
    #             if(count == 0):
    #                 i = i.replace('[', '') 
    #                 i = i.replace(']', '')
    #                 i = i.replace('n', '') 
    #                 i = i.replace('\\' , '')
    #                 i = i.replace('\'' , '')
    #                 i = i.replace(' ' , '')
    #                 entry = i.split(',')
    #                 print("first entry")
    #                 for k in entry:
    #                     #print(k)
                        
    #                     new_file.write(k + '\n' )
    #                 count = 1
    #             else:
    #                 #continue
    #                 new_file.write(i)
                    
            
    # scraper = Scraper_Object()
    # matricule_df = load_matricule_numbers()
    # ids = prepare_matricule(ids = matricule_df.MATRICULE83)
    # if len(sys.argv) > 1:
    #     batch_size = int(sys.argv[1])
    #     start = int(sys.argv[2])
    #     stop = int(sys.argv[3])
    # else:
    #     batch_size = 1000
    #     start = 0
    #     stop = len(matricule_df)
    # runner = Scraper_Runner(matricule_df, ids, scraper, batch_size, start, stop)
    # runner.run()

    #select_all("sql/20201012-150935/mtl_properties.sqlite").fetch_all()
    #print(get_tables("sql/20201012-150935/mtl_properties.sqlite"))
    runner.resume_scraping('20201013-105510')

    #logs_folder = f'logs/{runner.timestr}'
    # logs_folder = f'logs/20200709-135659'
    # ids = set()
    # error_count = 0
    # with open(f"{logs_folder}/scraped.txt", "r") as file:
    #     for line in file:
    #         if(line in ids):
    #             error_count += 1
    #         else:
    #             ids.add(line)
    # print(error_count)
    #data = select_distinct 
if __name__ == "__main__":
    main()