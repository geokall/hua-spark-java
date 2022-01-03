package hua;

import java.io.Serializable;
import java.util.Objects;

public class MovieDTO implements Serializable {

    private Integer movieId;

    private String title;

    private String genres;

    public MovieDTO() {
    }

    public MovieDTO(Integer movieId, String title, String genres) {
        this.movieId = movieId;
        this.title = title;
        this.genres = genres;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieDTO movieDTO = (MovieDTO) o;
        return Objects.equals(movieId, movieDTO.movieId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId);
    }
}
