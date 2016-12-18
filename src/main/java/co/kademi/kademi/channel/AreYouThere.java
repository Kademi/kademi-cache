package co.kademi.kademi.channel;

import java.io.Serializable;

/**
 * Sort of a heart beat message.
 *
 * @author brad
 */
public class AreYouThere implements Serializable{
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "Are you there?";
    }


}
