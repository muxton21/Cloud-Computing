import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static String[] arrayPush(String item, String[] oldArray){
        // New array with an additional element
        String[] newArray = new String[oldArray.length + 1];
        // Copy all the elements from the initial array
        for(int k = 0; k < oldArray.length; k++){
            newArray[k] = oldArray[k];
        }

        // Assign the new element with any value
        newArray[newArray.length - 1] = item;
        // Set the new array to the initial array while disposing of the inital array
        oldArray = newArray;
        return oldArray;
    }

    private static String[][] arrayPushArrayOfArrays(String[] item, String[][] oldArray){
        // New array with an additional element
        String[][] newArray = new String[oldArray.length +1][];
        // Copy all the elements from the initial array to the new array
        for(int i=0; i < oldArray.length; i++){
            newArray[i] = oldArray[i];
        }

        // Assign the new element with any value
        newArray[newArray.length - 1] = item;
        // Set the new array to the initial array while disposing of the initial array
        oldArray = newArray;
        //return old array with new element pushed
        return oldArray;
    }

    //Function checks if a string is present in an array of strings
    private static Boolean isStringInArray(String item, String[] array){
        //cycle through input array
        for(int i=0;i<array.length;i++){
            //boolean if item is present
            //Boolean stringEqual = item.equals(array[i]);
            //if(stringEqual || array[i] == null){
            if(item.equals(array[i])){
                return true;
            }
        }
        return false;
    }




    //function alphabetises an array of strings
    private static String[] alphabetiseArray(String[] array){
        Arrays.sort(array);
        return array;
    }

    private static String alphabetiseWord(String word){
        String[] wordArray = word.split("");
        Arrays.sort(wordArray);
        String joinedArray = String.join("", wordArray);
        return joinedArray;
    }

    private static String alphabetiseWord2(String word){
        String[] wordArray = word.split("");
        Arrays.sort(wordArray);
        String string = "";
        for(int i=0;i<wordArray.length;i++){
            string += wordArray[i];
        }
        return string;
    }
    private static String[] removeElementFromArray(String element, String[] oldArray){

        List<String> newList = new ArrayList<String>(Arrays.asList(oldArray));
        newList.remove(element);
        String[] newArray = newList.toArray(new String[0]);
        return newArray;
    }

    private static String[] checkForDuplicates(String[] array, String[] resultArray){
        //removes all duplicate words from array to leave only unique values
        for(int i=0;i<array.length;i++){
            //if value in split string is in the result array
            Boolean condition = isStringInArray(array[i], resultArray);
            if(!condition){
                resultArray = arrayPush(array[i], resultArray);
            }
        }
        return resultArray;
    }


    //Function that creates an array of tuples for all the words that match onto each other in the array
    //of all words in the .txt file
    private static String[][] arraySelfMatch(String[] alphabetisedWordsArray, String[] preFormattedWordsArray){

        String[][] anagramsArray = {};
        while(preFormattedWordsArray.length > 0){
            String selectedWord = preFormattedWordsArray[0];
            String selectedWordInLetterOrder = alphabetisedWordsArray[0];

            //remove the selected word from both the in order words array and words in letter order array
            preFormattedWordsArray = removeElementFromArray(selectedWord, preFormattedWordsArray);
            alphabetisedWordsArray = removeElementFromArray(selectedWordInLetterOrder, alphabetisedWordsArray);

            //add the selected word to the anagrams array
            String[] anagrams = {selectedWord};
            String[] anagramsInLetterOrder = {selectedWordInLetterOrder};


            //cycle through results array to identify anagram matches
            for(int i=0;i<alphabetisedWordsArray.length;i++){
                //if the words match add the ordered word to the anagrams array and remove it from both
                // the ordered word and letter order words arrays
                if(selectedWordInLetterOrder.equals(alphabetisedWordsArray[i])) {
                    //array is in resultArray add to anagrams array and remove word from array
                    anagrams = arrayPush(preFormattedWordsArray[i], anagrams);
                    anagramsInLetterOrder = arrayPush(alphabetisedWordsArray[i], anagramsInLetterOrder);
                }
            }
            //remove the elements used for anagrams from the overall arrays
            if(anagrams.length > 1 && anagramsInLetterOrder.length > 1){
                for (int j=0;j<anagrams.length;j++){
                    preFormattedWordsArray = removeElementFromArray(anagrams[j], preFormattedWordsArray);
                    alphabetisedWordsArray = removeElementFromArray(anagramsInLetterOrder[j], alphabetisedWordsArray);
                }
            }
            if(anagrams.length > 1){
                anagramsArray = arrayPushArrayOfArrays(anagrams, anagramsArray);
            }

        }
        return anagramsArray;
    }



    public static void main(String[] args) throws IOException {


        //initialise the result array of unique words
        String[] resultArray = {};


        //get text from .txt files for analysis and make it lower case
        String string = "";
        Scanner url = new Scanner(new URL("http://www.gutenberg.org/files/46/46-0.txt").openStream());
        while (url.hasNext()){
            string += url.nextLine() + " ";
        }
        url.close();
        //String string = new String(Files.readAllBytes(Paths.get("/Users/matthew/IdeaProjects/word_count/txt_files/bible.txt")), StandardCharsets.UTF_8).toLowerCase();
        //splits words into an array for any character that is not alphanumeric
        String[] splitString = string.split("[^a-zA-Z'\"]");
        //remove any punctuation
        for(int i=0;i<splitString.length;i++){
            splitString[i] = splitString[i].toLowerCase().replaceAll("[^a-z ]", "");
        }
        System.out.println(splitString.length);


        resultArray = checkForDuplicates(splitString, resultArray);
        Arrays.sort(resultArray);
        for(int i=0;i<resultArray.length;i++){
            System.out.println(resultArray[i]);
        }

        /*
        //arrange all elements in array in alphabetical ordered
        resultArray = alphabetiseArray(resultArray);

        //initialise array of ordered words for later referencing
        String[] orderedWords = new String[resultArray.length];
        System.arraycopy(resultArray,0, orderedWords, 0 , resultArray.length);




        //arrange all words in letter order
        for(int i=0;i<resultArray.length;i++){
            resultArray[i] = alphabetiseWord(resultArray[i]);
        }
        resultArray = removeElementFromArray("", resultArray);
        orderedWords = removeElementFromArray("", orderedWords);
        System.out.println(Arrays.toString(resultArray));
        System.out.println(Arrays.toString(resultArray).length());
        System.out.println(Arrays.toString(orderedWords));
        //form an array of matching words in array of all words in .txt file
        String[][] resultArrayOrderedMatches = arraySelfMatch(resultArray, orderedWords);


        //output array of matching anagram words line by line
        System.out.println("Anagrams: "+resultArrayOrderedMatches.length);
        for(int i=0;i<resultArrayOrderedMatches.length;i++){
            System.out.println(Arrays.toString(resultArrayOrderedMatches[i]));
        }
        */

            //System.out.println(Arrays.toString(resultArray));


    }
}