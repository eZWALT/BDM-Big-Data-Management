from typing import Callable, Optional

from atproto import models
from atproto_client.models import blob_ref as blob_models


def print_viewer_state(
    viewer: models.AppBskyActorDefs.ViewerState,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the viewer state.

    Args:
        viewer (models.AppBskyActorDefs.ViewerState): The viewer state to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Blocked By: {viewer.blocked_by}")
    _print(f"Blocking: {viewer.blocking}")
    _print(f"Blocking By List: {viewer.blocking_by_list}")
    _print(f"Followed By: {viewer.followed_by}")
    _print(f"Following: {viewer.following}")
    _print(f"Known Followers: {viewer.known_followers}")
    _print(f"Muted: {viewer.muted}")
    _print(f"Muted By List: {viewer.muted_by_list}")
    _print(f"Py Type: {viewer.py_type}")


def print_profile_basic(
    profile: models.AppBskyActorDefs.ProfileViewBasic,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the basic details of a profile.

    Args:
        profile (models.AppBskyActorDefs.ProfileViewBasic): The profile to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Did: {profile.did}")
    _print(f"Handle: {profile.handle}")
    _print(f"Associated: {profile.associated}")
    _print(f"Avatar: {profile.avatar}")
    _print(f"Created At: {profile.created_at}")
    _print(f"Display Name: {profile.display_name}")
    _print(f"Labels: {profile.labels}")
    _print(f"Viewer:")
    print_viewer_state(profile.viewer, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {profile.py_type}")


def print_ipld_link(
    link: blob_models.IpldLink,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the IPLD link details.

    Args:
        link (models.IpldLink): The IPLD link to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Link: {link.link}")


def print_blob_ref(
    blob_ref: blob_models.BlobRef,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the blob reference details of an external embed.

    Args:
        blob_ref (models.AppBskyEmbedExternal.BlobRef): The blob reference to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Mime Type: {blob_ref.mime_type}")
    _print(f"Size: {blob_ref.size}")
    _print(f"Ref:")
    print_ipld_link(blob_ref.ref, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {blob_ref.py_type}")


def print_embed_external_external(
    view: models.AppBskyEmbedExternal.External,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the view details of an external embed.
    Args:
        view (models.AppBskyEmbedExternal.View): The view details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Description: {view.description}")
    _print(f"Title: {view.title}")
    _print(f"Uri: {view.uri}")
    if view.thumb:
        _print(f"Thumb:")
        print_blob_ref(view.thumb, print_fn=print_fn, indent=indent + 4)
    else:
        _print("Thumb: None")
    _print(f"Py Type: {view.py_type}")


def print_embed_external_main(
    embed: models.AppBskyEmbedExternal.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"External:")
    print_embed_external_external(embed.external, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {embed.py_type}")


def print_embed_external_view_external(
    embed: models.AppBskyEmbedExternal.ViewExternal,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed external details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Description: {embed.description}")
    _print(f"Title: {embed.title}")
    _print(f"Uri: {embed.uri}")
    _print(f"Thumb: {embed.thumb}")
    _print(f"Py Type: {embed.py_type}")


def print_embed_external_view(
    embed: models.AppBskyEmbedExternal.View,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"External:")
    print_embed_external_view_external(embed.external, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {embed.py_type}")


def print_embed_image(
    image: models.AppBskyEmbedImages.Image,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the image details of an images embed.
    Args:
        image (models.AppBskyEmbedImages.Image): The image details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))

    _print(f"Alt: {image.alt}")
    _print(f"Aspect Ratio: {image.aspect_ratio}")
    _print(f"Image:")
    print_blob_ref(image.image, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {image.py_type}")


def print_embed_images_main(
    embed: models.AppBskyEmbedImages.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of an images embed.
    Args:
        embed (models.AppBskyEmbedImages.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Images:")
    for j, image in enumerate(embed.images):
        _print(f"    Image {j}:")
        print_embed_image(image, print_fn=print_fn, indent=indent + 8)
    _print(f"Py Type: {embed.py_type}")


def print_embed_image_view_image(
    image: models.AppBskyEmbedImages.ViewImage,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the image details of an images embed.
    Args:
        image (models.AppBskyEmbedImages.Image): The image details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))

    _print(f"Alt: {image.alt}")
    _print(f"Aspect Ratio: {image.aspect_ratio}")
    _print(f"Fullsize: {image.fullsize}")
    _print(f"Thumb: {image.thumb}")
    _print(f"Py Type: {image.py_type}")


def print_embed_images_view(
    embed: models.AppBskyEmbedImages.View,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of an images embed.
    Args:
        embed (models.AppBskyEmbedImages.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Images:")
    for j, image in enumerate(embed.images):
        _print(f"    Image {j}:")
        print_embed_image_view_image(image, print_fn=print_fn, indent=indent + 8)
    _print(f"Py Type: {embed.py_type}")


def print_embed_video_caption(
    caption: models.AppBskyEmbedVideo.Caption,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the caption details of a video embed.
    Args:
        caption (models.AppBskyEmbedVideo.Caption): The caption details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"File:")
    print_blob_ref(caption.file, print_fn=print_fn, indent=indent + 4)
    _print(f"Lang: {caption.lang}")
    _print(f"Py Type: {caption.py_type}")


def print_embed_video_main(
    embed: models.AppBskyEmbedVideo.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a video embed.
    Args:
        embed (models.AppBskyEmbedVideo.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Video:")
    print_blob_ref(embed.video, print_fn=print_fn, indent=indent + 4)
    _print(f"Alt: {embed.alt}")
    _print(f"Aspect Ratio: {embed.aspect_ratio}")
    _print(f"Captions:")
    for j, caption in enumerate(embed.captions):
        _print(" " * (indent + 4) + f"Caption {j}:")
        print_embed_video_caption(caption, print_fn=print_fn, indent=indent + 8)
    _print(f"Py Type: {embed.py_type}")


def print_embed_video_view(
    embed: models.AppBskyEmbedVideo.View,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a video embed.
    Args:
        embed (models.AppBskyEmbedVideo.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Cid: {embed.cid}")
    _print(f"Playlist: {embed.playlist}")
    _print(f"Alt: {embed.alt}")
    _print(f"Aspect Ratio: {embed.aspect_ratio}")
    _print(f"Thumbnail: {embed.thumbnail}")
    _print(f"Py Type: {embed.py_type}")


def print_com_atproto_repo_strong_ref(
    ref: models.ComAtprotoRepoStrongRef.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the strong reference details of a record embed.
    Args:
        ref (models.ComAtprotoRepoStrongRef.Main): The strong reference to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Cid: {ref.cid}")
    _print(f"Uri: {ref.uri}")
    _print(f"Py Type: {ref.py_type}")


def print_embed_record_view(
    embed: models.AppBskyEmbedRecord.View,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a record embed.
    Args:
        embed (models.AppBskyEmbedRecord.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Record: {embed.record}")
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_main(
    embed: models.AppBskyEmbedRecord.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a record embed.
    Args:
        embed (models.AppBskyEmbedRecord.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Record:")
    print_com_atproto_repo_strong_ref(embed.record, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_with_media_view(
    embed: models.AppBskyEmbedRecordWithMedia.View,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a record with media embed.
    Args:
        embed (models.AppBskyEmbedRecordWithMedia.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Media:")
    if isinstance(embed.media, models.AppBskyEmbedImages.View):
        print_embed_images_view(embed.media, print_fn=print_fn, indent=indent + 4)
    elif isinstance(embed.media, models.AppBskyEmbedVideo.View):
        print_embed_video_view(embed.media, print_fn=print_fn, indent=indent + 4)
    elif isinstance(embed.media, models.AppBskyEmbedExternal.View):
        print_embed_external_view(embed.media, print_fn=print_fn, indent=indent + 4)
    _print(f"Record:")
    print_embed_record_view(embed.record, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_with_media_main(
    embed: models.AppBskyEmbedRecordWithMedia.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the embed main details of a record with media embed.
    Args:
        embed (models.AppBskyEmbedRecordWithMedia.Main): The embed details to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Media:")
    if isinstance(embed.media, models.AppBskyEmbedImages.Main):
        print_embed_images_main(embed.media, print_fn=print_fn, indent=indent + 4)
    elif isinstance(embed.media, models.AppBskyEmbedVideo.Main):
        print_embed_video_main(embed.media, print_fn=print_fn, indent=indent + 4)
    elif isinstance(embed.media, models.AppBskyEmbedExternal.Main):
        print_embed_external_main(embed.media, print_fn=print_fn, indent=indent + 4)
    _print(f"Record:")
    print_embed_record_main(embed.record, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {embed.py_type}")


def print_byte_slice(
    byte_slice: models.AppBskyRichtextFacet.ByteSlice,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the byte slice details.

    Args:
        byte_slice (models.AppBskyRichtextFacet.ByteSlice): The byte slice to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Byte End: {byte_slice.byte_end}")
    _print(f"Byte Start: {byte_slice.byte_start}")
    _print(f"Py Type: {byte_slice.py_type}")


def print_rich_text_facet_link(
    facet: models.AppBskyRichtextFacet.Link,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the link details of a facet.

    Args:
        facet (models.AppBskyRichtextFacet.Link): The facet to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Uri: {facet.uri}")
    _print(f"Py Type: {facet.py_type}")


def print_rich_text_facet_main(
    facet: models.AppBskyRichtextFacet.Main,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the main details of a facet.

    Args:
        facet (models.AppBskyRichTextFacet.Main): The facet to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Features:")
    for j, feature in enumerate(facet.features):
        _print(f"    Feature {j}:")
        if isinstance(feature, models.AppBskyRichtextFacet.Link):
            print_rich_text_facet_link(feature, print_fn=print_fn, indent=indent + 8)
        else:
            _print(f"        Unknown feature type: {type(feature)}")
            _print(f"        Py Type: {feature.py_type}")
    _print(f"Index:")
    print_byte_slice(facet.index, print_fn=print_fn, indent=indent + 4)
    _print(f"Py Type: {facet.py_type}")


def print_post_record(
    record: models.AppBskyFeedPost.Record,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the details of a record.

    Args:
        record (models.AppBskyFeedDefs.Post): The record to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Created At: {record.created_at}")
    _print(f"Text: {record.text}")
    _print(f"Embed:")
    if isinstance(record.embed, models.AppBskyEmbedExternal.Main):
        print_embed_external_main(record.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(record.embed, models.AppBskyEmbedImages.Main):
        print_embed_images_main(record.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(record.embed, models.AppBskyEmbedVideo.Main):
        print_embed_video_main(record.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(record.embed, models.AppBskyEmbedRecord.Main):
        print_embed_record_main(record.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(record.embed, models.AppBskyEmbedRecordWithMedia.Main):
        print_embed_record_with_media_main(record.embed, print_fn=print_fn, indent=indent + 4)
    else:
        print_fn(" " * (indent + 4) + f"Unknown embed type: {type(record.embed)}")
    _print(f"Entities: {record.entities}")
    if record.facets:
        _print(f"Facets:")
        for j, facet in enumerate(record.facets):
            _print(f"    Facet {j}:")
            print_rich_text_facet_main(facet, print_fn=print_fn, indent=indent + 8)
    else:
        _print(f"Facets: None")
    _print(f"Labels: {record.labels}")
    _print(f"Langs: {record.langs}")
    _print(f"Reply: {record.reply}")
    _print(f"Tags: {record.tags}")
    _print(f"Py Type: {record.py_type}")


def print_post(
    post: models.AppBskyFeedDefs.PostView,
    print_fn: Optional[Callable[[str], None]] = print,
    indent: int = 0,
):
    """
    Print the details of a post.

    Args:
        post (models.AppBskyFeedDefs.PostView): The post to print.
    """
    _print = lambda x: print_fn(" " * indent + str(x))
    _print(f"Author:")
    print_profile_basic(post.author, print_fn=print_fn, indent=indent + 4)
    _print(f"Cid: {post.cid}")
    _print(f"Embed:")
    if isinstance(post.embed, models.AppBskyEmbedExternal.View):
        print_embed_external_view(post.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(post.embed, models.AppBskyEmbedImages.View):
        print_embed_images_view(post.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(post.embed, models.AppBskyEmbedVideo.View):
        print_embed_video_view(post.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(post.embed, models.AppBskyEmbedRecord.View):
        print_embed_record_view(post.embed, print_fn=print_fn, indent=indent + 4)
    elif isinstance(post.embed, models.AppBskyEmbedRecordWithMedia.View):
        print_embed_record_with_media_view(post.embed, print_fn=print_fn, indent=indent + 4)
    else:
        _print(" " * (indent + 4) + f"Unknown embed type: {type(post.embed)}")
    _print(f"Indexed At: {post.indexed_at}")
    _print(f"Labels: {post.labels}")
    _print(f"Like Count: {post.like_count}")
    _print(f"Py Type: {post.py_type}")
    _print(f"Quote Count: {post.quote_count}")
    _print(f"Record:")
    print_post_record(post.record, print_fn=print_fn, indent=indent + 4)
    _print(f"Reply Count: {post.reply_count}")
    _print(f"Repost Count: {post.repost_count}")
    _print(f"Threadgate: {post.threadgate}")
    _print(f"URI: {post.uri}")
    _print(f"Viewer: {post.viewer}")
