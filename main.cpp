#include <iostream> //printing
#include <fstream> //file
#include <algorithm> //utility
#include <utility> //utility
#include <thread> //multi-threading
#include <mutex> //access control
#include <atomic> //access control
#include <chrono> //time
#include <string> //htmls and domains
#include <list> //list for domains
#include <unordered_set> //sets to avoid repetition
#include <cstring> //c-strings
#include <include/CkSpider.h> //spider

#define URL_LIMIT 6000

//globals
std::atomic<int> g_fetched_urls_counter(0), g_active_threads(0), g_total_size(0),g_crawled_lvl0(0); //counters
std::atomic<double> g_total_time(0); //total time
std::atomic<bool> g_reached_limit=false;
std::list<std::string> g_domains; //domains to spider
std::unordered_set<std::string> g_visited[2],g_domains_being_crawled; //urls visited
std::string g_dir; //directory to save htmls
std::wofstream g_urls_output_file; //file to save urls and temporary htmls
std::mutex g_domains_lock; //mutex for list
std::mutex g_visited_lock[2],g_domains_being_crawled_lock; //mutexes to lock sets
std::mutex g_urls_output_file_lock; //mutex to save urls in file
std::mutex g_cout_block,g_cur_thread;
std::mutex g_low_priority_domain_lock,g_next_domain_lock;
std::atomic<int> g_ended_normal(0),g_reached(0);

//crawl and get data from the first unspidered
int Crawl_Calculations(CkSpider& spider,int x,int k)
{
    auto t1 = std::chrono::high_resolution_clock::now(); //before
    bool success = spider.CrawlNext(); //crawling
    auto t2 = std::chrono::high_resolution_clock::now(); //after
    g_visited_lock[1].lock(); //lock mutex
    if(!g_visited[1].emplace(spider.lastUrl()).second) //try to insert
    {
        g_visited_lock[1].unlock();
        return -1;
    }
    //std::cout<<"Inseriu\n\n";
    g_visited_lock[1].unlock();
    if (success) 
    {
        int cur_counter = ++g_fetched_urls_counter; //update counter
        if (!g_reached_limit) //if within limit
        {
            if (cur_counter>URL_LIMIT) //for performance boost, ensures that other threads are killed asap
            {
                //std::cout<<"Limite atingido\n";
                g_reached_limit=true;
                return 1;
            }
            std::string file_path(g_dir+"/"+std::to_string(cur_counter)); 
            std::ofstream output(file_path+".html"); //opening file for html
            char mybuff[200000]; //buffer for stream
            output.rdbuf()->pubsetbuf(mybuff,200000); //setting buffer
            output.write(spider.lastHtml(),strlen(spider.lastHtml())); //getting full html
            output.close();
            g_total_size += strlen(spider.lastHtml()); //getting html size
            double add_time=g_total_time; //this and next line are a workaround for atomic double sums
            g_total_time.compare_exchange_weak(add_time,add_time+std::chrono::duration_cast<std::chrono::duration<double>>(t2-t1).count()); //calculating duration
            std::scoped_lock<std::mutex> lk (g_urls_output_file_lock);
            g_urls_output_file << spider.lastUrl()<<std::endl;;
            return 2;
        }
        g_cout_block.lock();
        //std::cout<<"Recusou por limite\n";
        g_cout_block.unlock();
        return 1;
    }
    return 0;
}
void Crawl_Lvl_0(std::string base_site,int x)
{
    //initialization
    //std::unique_lock<std::mutex> lockzin(g_cur_thread);
    //g_cout_block.lock();
    //std::cout<<"Starting "<<x<<" with base="<<base_site<<"\n";;
    //g_cout_block.unlock();
    CkSpider spider;
    auto base_site_c_str=base_site.c_str();
    spider.Initialize(base_site_c_str);
    std::string cur_domain(spider.getBaseDomain(spider.domain()));
    spider.put_Utf8(true);
    spider.AddAvoidPattern(".pdf");
    spider.AddAvoidPattern("*.whatsapp.com*");
    spider.AddMustMatchPattern("*.br*");
    spider.AddMustMatchPattern("*.globo.com*");
    spider.put_MaxResponseSize(2*1024*1024); // 2mb
    std::unique_lock<std::mutex> lk (g_domains_being_crawled_lock);
    //std::cout<<"block crawled:"<<x<<"\n";;
    if (!g_domains_being_crawled.emplace(cur_domain).second) //domain is being crawled
    {
        lk.unlock();
        //g_cout_block.lock();
        //std::cout<<"being crawled on "<<x<<"\n";;
        //std::cout<<cur_domain<<"\n";
        //std::cout<<base_site<<"\n\n";
        g_visited_lock[0].lock();
        g_visited[0].erase(base_site);
        g_visited_lock[0].unlock();
        //g_cout_block.unlock();
        g_low_priority_domain_lock.lock();
        g_next_domain_lock.lock();
        g_domains_lock.lock();
        g_next_domain_lock.unlock();
        //g_cout_block.lock();
        //std::cout<<"block domains:"<<x<<"\n";;
        //g_cout_block.unlock();
        //g_cout_block.lock();
        //std::cout<<"proc: "<<x<<"\n";;
        //std::cout<<"antes de inserir:"<<g_domains.size()<<"\n";;
        g_domains.emplace_back(base_site);
        //std::cout<<"depois de inserir:"<<g_domains.size()<<"\n";;
        //g_cout_block.unlock();
        g_domains_lock.unlock();
        g_low_priority_domain_lock.unlock();
        //g_cout_block.lock();
        //std::cout<<"unblock domains:"<<x<<"\n";;
        //g_cout_block.unlock();
        //auto it = g_domains.begin();
        //std::advance(it,std::min(g_domains.size(),(size_t)5)); //rearrange list
        
        
        //g_domains.insert(it,base_site);
        g_active_threads--;
        return;
    }
    lk.unlock();
    //g_cout_block.lock();
    //std::cout<<"unblock crawled:"<<x<<"\n";;
    //g_cout_block.unlock();

    if(strcmp(base_site_c_str,spider.getBaseDomain(base_site_c_str)))
    {
        spider.AddUnspidered(base_site_c_str);
    }    

    //getting list of lvls 0 and 1 links
    auto t1 = std::chrono::high_resolution_clock::now(); //before
    bool success = spider.CrawlNext(); //crawling
    auto t2 = std::chrono::high_resolution_clock::now(); //after
    g_cout_block.lock();
    //std::cout<<"crawled "<<x<<" with value:"<<success<<"\n";;
    g_cout_block.unlock();
    
    if (!success || g_reached_limit)
    {
        g_domains_being_crawled_lock.lock();
        g_domains_being_crawled.erase(cur_domain);
        g_domains_being_crawled_lock.unlock();
        g_active_threads--;
        return;
    }
    g_crawled_lvl0++;
    int number_of_level_1=std::min(spider.get_NumUnspidered(),50); //limit to avoid going too deep in one domain
    int number_of_level_0=std::min(spider.get_NumOutboundLinks(),40); //same

    //inserting lvl 0 in domains list
    g_low_priority_domain_lock.lock();
    g_next_domain_lock.lock();
    g_domains_lock.lock();
    g_next_domain_lock.unlock();
    //g_cout_block.lock();
    //std::cout<<"block domains:"<<x<<"\n";;
    //g_cout_block.unlock();
    //g_cout_block.lock();
    //std::cout<<x<<" added 0:"<<number_of_level_0<<"\n";;
    //g_cout_block.unlock();
    int i;
    for(i=0;i<number_of_level_0 && g_domains.size()<URL_LIMIT/2 && !g_reached_limit;i++)
    {
        g_domains.push_back(spider.getOutboundLink(i));
    }
    g_cout_block.lock();
    //std::cout<<x<<" added 0:"<<number_of_level_0<<"\n\n";;
    //std::cout<<x<<" added 1:"<<number_of_level_1<<"\n";;
    g_cout_block.unlock();
    g_domains_lock.unlock();
    g_low_priority_domain_lock.unlock();
    g_cout_block.lock();
    //std::cout<<"unblock domains:"<<x<<"\n";;
    g_cout_block.unlock();

    //getting htmls and urls from lvl1
    int func_return;
    for(int i=0;i<number_of_level_1;i++) 
    {
        func_return = Crawl_Calculations(spider,x,1);
        switch (func_return)
        {
            case 1: //reached limits, so kill the thread
            {
                g_cout_block.lock();
                //std::cout<<"reached limit2: "<<x<<"\n";;
                //std::cout<<cur_domain<<"\n\n";
                g_cout_block.unlock();
                lk.lock();
                //g_cout_block.lock();
                //std::cout<<"block crawled:"<<x<<"\n";;
                //g_cout_block.unlock();
                g_domains_being_crawled.erase(cur_domain);
                lk.unlock();
                //g_cout_block.lock();
                //std::cout<<"After erase: "<<g_domains_being_crawled.count(cur_domain)<<"\n\n";
                //g_cout_block.unlock();
                g_active_threads--;
                g_reached++;
                
                //g_cout_block.lock();
                //std::cout<<"unblock crawled:"<<x<<"\n";;
                //g_cout_block.unlock();
                return;
            }
            default: //got page or failed
            {
                break;
            }
        } 
        spider.SleepMs(100); //sleep 0.1 second before spidering the next URL to avoid flood.
    }

    //ended this domain
    g_cout_block.lock();
    //std::cout<<"ended normal: "<<x<<"\n";;
    //std::cout<<cur_domain<<"\n\n";
    g_cout_block.unlock();
    g_ended_normal++;
    g_domains_being_crawled_lock.lock();
    g_domains_being_crawled.erase(cur_domain);
    g_domains_being_crawled_lock.unlock();
    //g_cout_block.lock();
    //std::cout<<"block crawled:"<<x<<"\n";;
    //g_cout_block.unlock();
    //g_cout_block.lock();
    //std::cout<<"After erase: "<<g_domains_being_crawled.count(cur_domain)<<"\n\n";
    //g_cout_block.unlock();
    g_active_threads--;
    //g_cout_block.lock();
    //std::cout<<"unblock crawled:"<<x<<"\n";;
    //g_cout_block.unlock();
    return;
}

int main(int argc, char** argv)
{
    if (argc<3) //check arguments
    {
        //std::cout<<"Input file or directory name not informed. Program will end.\n";
        exit(-1);
    }
    auto t1=std::chrono::high_resolution_clock::now(); //initial time_point
    std::ifstream input_file(argv[1]); //opening input file with links
    g_dir=argv[2]; //getting dir to save files
    g_urls_output_file.open(g_dir+"/output_links.txt"); //opening file to write links
    std::string temp;
    while (input_file>>temp) //insert links in list
        g_domains.push_back(temp);
    wchar_t buffer[200000];
    g_urls_output_file.rdbuf()->pubsetbuf(buffer,200000);
    int x=1;
    std::unique_lock<std::mutex> g_domains_lock_guard (g_domains_lock,std::defer_lock);
    while((g_domains.size()!=0 || g_active_threads!=0) && !g_reached_limit) //loop until there's something in queue or processing
    {
        g_next_domain_lock.lock();
        g_domains_lock_guard.lock();
        g_next_domain_lock.unlock();
        g_cout_block.lock();
        //std::cout<<"block domains: main"<<"\n";;
        //g_cout_block.lock();
        std::cout<<"Active="<<g_active_threads<<"\n";; //progress bar
        std::cout<<"Pending="<<g_domains.size()<<"\n";;
        //std::cout<<"Reached="<<g_reached_limit<<"\n";;
        //g_cout_block.unlock();
        
        while(!g_domains.empty() && g_active_threads<50 && !g_reached_limit) //prioritize lvl0 (bfs)
        {
            g_visited_lock[0].lock();
            //std::cout<<"Tentando inserir na main: "<<x<<"\n";;
            if(!g_visited[0].emplace(g_domains.front()).second) //avoid domain repetition
            {
                //std::cout<<"Falhou: "<<g_domains.front()<<"\n\n";
                g_domains.pop_front();
                g_visited_lock[0].unlock();
                continue;
            }
            g_visited_lock[0].unlock();
            std::thread(Crawl_Lvl_0,g_domains.front(),x++).detach(); //spawn thread for current domain
            g_domains.pop_front();
            g_active_threads++;
        }
        //std::cout<<"After unloading: "<<g_active_threads<<" active and "<<g_domains.size()<<" pending\n"<<"\n";;
        g_cout_block.unlock();
        g_domains_lock_guard.unlock();
        //g_cout_block.lock();
        //std::cout<<"unblock domains: main"<<"\n";;
        //g_cout_block.unlock();
        if (g_fetched_urls_counter>=URL_LIMIT) //reached limit
            break;
        g_urls_output_file_lock.lock();
        g_urls_output_file.flush();
        g_urls_output_file_lock.unlock();
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    while (g_active_threads>0) //wait for active threads to finish
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    //std::cout<<"-------------------------------------------------\n";
    //int total = g_visited.size();
    std::cout<<"Number of level 0: "<< g_visited[0].size()<<"\n";;
    std::cout<<"Number of level 1: "<< g_visited[1].size()<<"\n";;
    std::cout<<"Number of level 0 actually crawled: "<<g_crawled_lvl0<<"\n";;
    //std::cout<<"Average time per crawl: "<<g_total_time/total<<"s\n";
    //std::cout<<"Average number of characters per crawl: "<<g_total_size/total<<"\n";;
    //std::cout<<"Reached limit: "<<g_reached<<"\n";;
    //std::cout<<"Ended normal: "<<g_ended_normal<<"\n";;
    auto t2=std::chrono::high_resolution_clock::now();
    std::cout<<"Total time spent: "<<std::chrono::duration_cast<std::chrono::duration<double>>(t2-t1).count()<<"s\n\n";
    //for(auto &i:g_visited)
        //std::cout<<i<<"\n";;
    return 0;
}