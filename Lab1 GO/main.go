package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var (
	DataSize = 1000
)

type Students struct {
	Students []Student `json:"students"`
}
type Student struct {
	Name  string    `json:"name"`
	Year  int       `json:"year"`
	Grade float32   `json:"grade"`
	Gender string   `json:"gender"`
	UserName string `json:"username
"`
}
func (student *Student) Compare(other *Student) bool{
	if student.Year == other.Year {
		return student.Grade >  other.Grade
	} else {
		return student.Year > other.Year
	}
}
type Result struct {
	Student *Student
	ResultValue [32]byte // fancy byte hash for now
}

type ResultMonitor struct {
	DataArray         []Result
	Count             int
	cond              *sync.Cond
	mutex             *sync.Mutex
}

type DataMonitor struct {
	DataArray         []Student
	Count             int
	hasFinishedAdding bool
	cond              *sync.Cond
	mutex             *sync.Mutex
}

func CreateDataMonitor() *DataMonitor {
	mutex := sync.Mutex{}
	return &DataMonitor{Count: 0, cond: sync.NewCond(&mutex), mutex: &mutex, DataArray: make([]Student, DataSize / 2)}
}

func CreateResultMonitor() *ResultMonitor {
	mutex := sync.Mutex{}
	return &ResultMonitor{Count: 0, cond: sync.NewCond(&mutex), mutex: &mutex, DataArray: make([]Result, DataSize)}
}

func (masyvas *ResultMonitor) Add(result *Result) {

	masyvas.mutex.Lock()
	defer masyvas.mutex.Unlock()

	for index := 0; index < masyvas.Count; index++ {
		if masyvas.DataArray[index].Student.Compare(result.Student) {

			var oldRez Result
			newRez := *result
			for i := index; i < masyvas.Count + 1; i++ {
				oldRez = masyvas.DataArray[i]
				masyvas.DataArray[i] = newRez
				newRez = oldRez
			}
			masyvas.Count++
			return
		}
	}
	masyvas.DataArray[masyvas.Count] = *result
	masyvas.Count++
}

func (masyvas *DataMonitor) Add(student *Student){

	masyvas.mutex.Lock()
	defer masyvas.mutex.Unlock() // Unlock the critical section on function exit
	defer masyvas.cond.Signal()  // Wake a waiting thread to take the value out

	for masyvas.Count >= len(masyvas.DataArray) {
		// Put thread to sleep to wait for data to be taken out
		masyvas.cond.Wait()
	}

	masyvas.DataArray[masyvas.Count] = *student
	masyvas.Count++

}

func (masyvas *DataMonitor) Take() *Student{

	masyvas.mutex.Lock() // Lock the critical section

	defer masyvas.mutex.Unlock() // Unlock the critical section at the end
	defer masyvas.cond.Signal() // Notify a waiting thread about a taken value

	for masyvas.Count == 0 {
		if masyvas.hasFinishedAdding {
			// Indicates that no more values will be added so it will return with a signal to finish threads
			return nil
		}
		masyvas.cond.Wait()
	}

	stud := masyvas.DataArray[masyvas.Count - 1]
	masyvas.Count--

	return &stud
}

func (students *Students) ReadJsonStudents(fileName string){
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	byteValue, _ := ioutil.ReadAll(file)

	err = json.Unmarshal(byteValue, &students)
	if err != nil {
		panic(err)
	}
}

func VeryHeavyFunction(data *DataMonitor, result *ResultMonitor,wg *sync.WaitGroup){

	defer wg.Done()

	for { // Loops till the ResultMonitor is empty and no longer being added to
		stud := data.Take()
		if stud == nil {
			// No more work to be done, so its time to exit :^)
			break
		}

		stringToHash := fmt.Sprintf("%v %v %v %v %v %v", stud.Name, stud.Year, stud.Grade, stud.UserName, stud.Gender)
		var hash [32]byte
		hash = sha256.Sum256([]byte(stringToHash)) // Fancy hashing
		for i := 0; i < 1000; i++ {
			hash = sha256.Sum256([]byte(fmt.Sprintf("%v%x", i, hash)))
		}

		if stud.Grade >= 7 { // Very fancy filtering
			result.Add(&Result{Student: stud, ResultValue: hash})
		}

	}
}

func main(){

	t := time.Now()
	var students Students
	dataMonitor := CreateDataMonitor()
	resultMonitor := CreateResultMonitor()
	students.ReadJsonStudents("Data 1.json")
	wGroup := sync.WaitGroup{}
	threadCount := DataSize / 4
	wGroup.Add(threadCount)

	for i := 0; i < threadCount; i++ {
		go VeryHeavyFunction(dataMonitor, resultMonitor,&wGroup);
	}

	for _, stud := range students.Students { // Adding values to the DataMonitor
		dataMonitor.Add(&stud)
	}
	dataMonitor.hasFinishedAdding = true // Setting a flag that no more values will be added

	wGroup.Wait()

	// Creating and writing the result file

	resultFile, err := os.OpenFile("rez 1.txt",os.O_TRUNC | os.O_WRONLY | os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer resultFile.Close()

	resultFile.WriteString(fmt.Sprintf("Name: %-12v |UserName: %-10v |Year: %-5v |Grade: %-5v |Hash: %v\n","", "","","",""))
	for i := 0; i < resultMonitor.Count; i++{
		resultFile.WriteString(fmt.Sprintf("%-18v |%-20v |%-11v |%-12v |%x\n",
			resultMonitor.DataArray[i].Student.Name,
			resultMonitor.DataArray[i].Student.UserName,
			resultMonitor.DataArray[i].Student.Year,
			resultMonitor.DataArray[i].Student.Grade,
			resultMonitor.DataArray[i].ResultValue))
	}
	fmt.Println("Total time taken:",time.Now().Sub(t))
	fmt.Println("I die peacefully")
}



