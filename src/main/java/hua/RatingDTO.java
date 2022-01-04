package hua;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

public class RatingDTO implements Serializable {

    private Integer userId;

    private Integer movieId;

    private Double rating;

    private LocalDateTime timeStampParsed;

    public RatingDTO() {
    }

    public RatingDTO(Integer userId, Integer movieId, Double rating, LocalDateTime timeStampParsed) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timeStampParsed = timeStampParsed;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public LocalDateTime getTimeStampParsed() {
        return timeStampParsed;
    }

    public void setTimeStampParsed(LocalDateTime timeStampParsed) {
        this.timeStampParsed = timeStampParsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatingDTO ratingDTO = (RatingDTO) o;
        return Objects.equals(movieId, ratingDTO.movieId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId);
    }
}