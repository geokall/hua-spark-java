package hua;

import java.io.Serializable;
import java.util.List;

public class MovieDTO implements Serializable {

    private Integer movieID;

    private String title;

    private List<String> genres;

    public MovieDTO(Integer movieID, String title, List<String> genres) {
        this.movieID = movieID;
        this.title = title;
        this.genres = genres;
    }

    public Integer getMovieID() {
        return movieID;
    }

    public void setMovieID(Integer movieID) {
        this.movieID = movieID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }
}
