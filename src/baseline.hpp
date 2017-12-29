#include <fstream>
#include <algorithm> // vector, utility
#include <iomanip> // chrono, string, iostream, sstream
#include <stdexcept>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/chrono.hpp>
#include <cereal/archives/binary.hpp>
#include <cereal/access.hpp>

using std::string;
using std::vector;

typedef std::chrono::system_clock::time_point time_point;


// ++++++++++ CLASS DECLARATIONS ++++++++++

class record
{
  public:
    // members
    time_point time;
    string page;
    size_t counter;
  
    // constructors
    record(); // empty
    record(const time_point&& time, const string&& page, const size_t counter); // move
    record(const string& source_line); // from source file line
    
    // sorting
    inline bool operator< (const record& x) const;
    
    // print
    inline string to_string() const;
    
    // helper
    static inline time_point string_to_time_point(const string& source, const string& format = "%Y%m%d-%H");
    static inline bool compare_page(const record& r1, const record& r2);
    
  private:
    // serialization with Cereal
    template <class Archive>
    void serialize(Archive& ar);
  
  // to allow Cereal access to the private methods
  friend class cereal::access;
};


class baseline_db
{
  private:
    // a simple vector of records, kept sorted
    vector<record> m_db;
  
  public:
    // no explicit constructor, intialization comes afterwards
    
    // to init from raw data
    inline void build_index(const string& source);
    
    // serialized file I/O
    inline void load(const string& source);
    inline void save_as(const string& dest) const;
    
    // queries
    // with times as time_point
    inline vector<record> range(const string& page, const time_point& time1, const time_point& time2) const;
    inline vector<record> top_k_range(const string& page, const time_point& time1, const time_point& time2, const size_t k) const;
    // with times as strings formatted like on the source
    inline vector<record> range(const string& page, const string& time1, const string& time2) const;
    inline vector<record> top_k_range(const string& page, const string& time1, const string& time2, const size_t k) const;
    
    // debug
    inline void print(const size_t i) const; // print i-th record
    inline void print_all() const; // print all records
  
  private:
    // returns range of corresponding records or <-1,-1> if none found
    inline std::pair<vector<record>::const_iterator,vector<record>::const_iterator> range_of(const string& page) const;
    
    // serialization with Cereal
    template <class Archive>
    void serialize(Archive& ar);
  
  // to allow Cereal access to the private methods
  friend class cereal::access;
};



// ++++++++++ CLASS IMPLEMENTATIONS ++++++++++

// +++++ RECORD +++++

record::record()
: time(), page(), counter() { }


record::record(const time_point&& time, const string&& page, const size_t counter)
: time(time), page(page), counter(counter) { }


record::record(const string& source_line)
// example of source string: 20160626-23	10_Cloverfield_Lane	475
{
  string::size_type n1, n2;
  n1 = source_line.find('\t');
  n2 = source_line.find('\t', n1+1);
  time = string_to_time_point(source_line.substr(0,n1));
  page = source_line.substr(n1+1,n2-n1-1);
  counter = std::stoi(source_line.substr(n2+1));
}


inline bool record::operator< (const record& x) const
{
  return std::tie(page, time) < std::tie(x.page, x.time);
}


inline time_point record::string_to_time_point(const string& source, const string& format)
{
  std::stringstream ss(source);
  std::tm tm = {};
  ss >> std::get_time(&tm, format.c_str());
  if (ss.fail()) {
    throw std::invalid_argument("record::string_to_time_point couldn't parse string \"" + source + "\" with format \"" + format + "\"");
  }
  return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}


inline string record::to_string() const
{
  std::stringstream ss;
  std::time_t tm = std::chrono::system_clock::to_time_t(time);
  ss << "time:" << std::put_time(std::localtime(&tm), "%Y%m%d-%H") << ",page:" << page << ",counter:" << counter << ".";
  return ss.str();
}


inline bool record::compare_page(const record& r1, const record& r2)
{
  return r1.page < r2.page;
}


template <class Archive>
void record::serialize(Archive& ar)
{
  ar(time, page, counter);
}



// +++++ BASELINE_DB +++++

void baseline_db::build_index(const string& source)
{
  string line;
  std::ifstream sourcefile;
  sourcefile.open(source);
  if (sourcefile.is_open()) {
    while (std::getline(sourcefile, line)) {
      m_db.push_back(record(line));
    }
    sourcefile.close();
    std::sort(m_db.begin(), m_db.end());
  } else {
    throw std::ios_base::failure("baseline_db::build_index couldn't open file " + source);
  }
}


void baseline_db::load(const string& source)
{
  std::ifstream sourcefile(source, std::ios::binary);
  cereal::BinaryInputArchive ar(sourcefile);
  ar(m_db);
}


void baseline_db::save_as(const string& dest) const
{
  std::ofstream destfile(dest, std::ios::binary);
  cereal::BinaryOutputArchive ar(destfile);
  ar(*this);
}


inline vector<record> baseline_db::range(const string& page, const time_point& time1, const time_point& time2) const
{
  vector<record> retval;
  if (time1 <= time2) {
    std::pair<vector<record>::const_iterator,vector<record>::const_iterator> range = range_of(page);
    // NB if range.first == range.second then there was no match found
    for (auto it = range.first; it < range.second; it++)
      if (time1 <= (*it).time && (*it).time <= time2) retval.push_back(*it); // TODO figure out if move or copy
  } else {
    std::stringstream ss;
    std::time_t tm1 = std::chrono::system_clock::to_time_t(time1);
    std::time_t tm2 = std::chrono::system_clock::to_time_t(time2);
    ss << "<" << std::put_time(std::localtime(&tm1), "%Y%m%d-%H") << "," << std::put_time(std::localtime(&tm2), "%Y%m%d-%H") << ">";
    throw std::invalid_argument("baseline_db::range malformed time interval " + ss.str());
  }
  return retval;
}


inline vector<record> baseline_db::top_k_range(const string& page, const time_point& time1, const time_point& time2, const size_t k) const
{
  vector<record> retval = range(page, time1, time2);
  std::sort(retval.begin(), retval.end());
  while (retval.size() > k) retval.pop_back();
  return retval;
}


inline vector<record> baseline_db::range(const string& page, const string& time1, const string& time2) const
{
  return range(page, record::string_to_time_point(time1), record::string_to_time_point(time2));
}


inline vector<record> baseline_db::top_k_range(const string& page, const string& time1, const string& time2, const size_t k) const
{
  return top_k_range(page, record::string_to_time_point(time1), record::string_to_time_point(time2), k);
}


inline std::pair<vector<record>::const_iterator,vector<record>::const_iterator> baseline_db::range_of(const string& page) const
{
  record dummy;
  dummy.page = page; // copy, not move
  return std::equal_range(m_db.cbegin(), m_db.cend(), dummy, record::compare_page);
}


inline void baseline_db::print(const size_t i) const
{
  std::cout << m_db[i].to_string() << std::endl;
}


inline void baseline_db::print_all() const
{
  for (auto it = m_db.cbegin(); it < m_db.cend(); it++) std::cout << (*it).to_string() << std::endl;
}

template <class Archive>
void baseline_db::serialize(Archive& ar)
{
  ar(m_db);
}